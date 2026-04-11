"""VRM bridge — polls Victron VRM diagnostics, writes Postgres, republishes to local Mosquitto."""

import json
import logging
import math
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import psycopg
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("vrm-bridge")

VRM_TOKEN = os.environ["VRM_TOKEN"]
VRM_PORTAL_ID = os.environ["VRM_PORTAL_ID"]
VRM_API_BASE = os.environ.get("VRM_API_BASE", "https://vrmapi.victronenergy.com/v2")
VRM_POLL_INTERVAL = int(os.environ.get("VRM_POLL_INTERVAL", os.environ.get("DB_WRITE_INTERVAL", "30")))
VRM_REQUEST_TIMEOUT = int(os.environ.get("VRM_REQUEST_TIMEOUT", "20"))

LOCAL_MQTT_HOST = os.environ.get("LOCAL_MQTT_HOST", "localhost")
LOCAL_MQTT_PORT = int(os.environ.get("LOCAL_MQTT_PORT", "1883"))
LOCAL_MQTT_PREFIX = os.environ.get("LOCAL_MQTT_PREFIX", "victron")
LOCAL_MQTT_USERNAME = os.environ.get("LOCAL_MQTT_USERNAME", "").strip()
LOCAL_MQTT_PASSWORD = os.environ.get("LOCAL_MQTT_PASSWORD", "").strip()

DATABASE_HOST = os.environ["DATABASE_HOST"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
DATABASE_USER = os.environ["DATABASE_USER"]
DATABASE_PASS = os.environ["DATABASE_PASS"]
DATABASE_PORT = int(os.environ.get("DATABASE_PORT", "5432"))

DB_WRITE_INTERVAL = int(os.environ.get("DB_WRITE_INTERVAL", "30"))

LAKEMATES_PUSH_URL = os.environ.get("LAKEMATES_PUSH_URL", "").strip()
LAKEMATES_SITE_KEY = os.environ.get("LAKEMATES_SITE_KEY", "").strip()
LAKEMATES_INGEST_SECRET = os.environ.get("LAKEMATES_INGEST_SECRET", "").strip()
LAKEMATES_PUSH_TIMEOUT = int(os.environ.get("LAKEMATES_PUSH_TIMEOUT", "10"))

HEALTH_FILE = Path("/tmp/healthy")

# ---------------------------------------------------------------------------
# Metric mapping: diagnostics identifier/path → friendly metric name
# Keep names stable for Postgres, Grafana, and local MQTT consumers.
# ---------------------------------------------------------------------------

METRIC_MAP: dict[str, str] = {
    # battery
    "battery_soc": "battery_soc",
    "battery_voltage": "battery_voltage",
    "battery_current": "battery_current",
    "battery_power": "battery_power",
    "Dc/Battery/Soc": "battery_soc",
    "Dc/Battery/Voltage": "battery_voltage",
    "Dc/Battery/Current": "battery_current",
    "Dc/Battery/Power": "battery_power",
    "battery/Soc": "battery_soc",
    "battery/Dc/0/Voltage": "battery_voltage",
    "battery/Dc/0/Current": "battery_current",
    "system/Dc/Battery/Power": "battery_power",
    "/Soc": "battery_soc",
    "/Dc/0/Voltage": "battery_voltage",
    "/Dc/0/Current": "battery_current",
    "/Dc/Battery/Power": "battery_power",

    # solar
    "solar_power": "solar_power",
    "solar_current": "solar_current",
    "Dc/Pv/Power": "solar_power",
    "Dc/Pv/Current": "solar_current",
    "system/Dc/Pv/Power": "solar_power",
    "solarcharger/Yield/Power": "solar_power",
    "solarcharger/Dc/0/Current": "solar_current",
    "/Yield/Power": "solar_power",

    # loads / state
    "ac_consumption_l1": "ac_consumption_l1",
    "ac_loads_on_output_l1": "ac_loads_on_output_l1",
    "system_state": "system_state",
    "Ac/Consumption/L1/Power": "ac_consumption_l1",
    "Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "SystemState/State": "system_state",
    "system/Ac/Consumption/L1/Power": "ac_consumption_l1",
    "system/Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "system/SystemState/State": "system_state",
    "/Ac/Consumption/L1/Power": "ac_consumption_l1",
    "/Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "/SystemState/State": "system_state",

    # source / inverter
    "ac_active_in_l1": "ac_active_in_l1",
    "Ac/ActiveIn/L1/P": "ac_active_in_l1",
    "vebus/Ac/ActiveIn/L1/P": "ac_active_in_l1",
    "/Ac/ActiveIn/L1/P": "ac_active_in_l1",
}

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

shutdown_event = threading.Event()
buffer_lock = threading.Lock()
metric_buffer: dict[str, dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Diagnostics helpers
# ---------------------------------------------------------------------------

def normalize_metric_key(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.strip("/")
    for candidate in (text, normalized, f"/{normalized}"):
        metric = METRIC_MAP.get(candidate)
        if metric:
            return metric
    return None


def extract_metric_name(item: dict[str, Any]) -> str | None:
    dbus_service = item.get("dbusServiceType") or item.get("dbus_service_type")
    dbus_path = item.get("dbusPath") or item.get("dbus_path") or item.get("path")

    composite_candidates: list[str] = []
    if dbus_service and dbus_path:
        composite_candidates.append(f"{dbus_service}{dbus_path}")
        composite_candidates.append(f"{dbus_service}/{str(dbus_path).lstrip('/')}")

    candidates = (
        *composite_candidates,
        dbus_path,
        item.get("code"),
        item.get("metric"),
        item.get("idDataAttribute"),
        item.get("attributeName"),
        item.get("name"),
    )
    for candidate in candidates:
        metric = normalize_metric_key(candidate)
        if metric:
            return metric
    return None


def coerce_value(value: Any) -> tuple[str | None, float | None]:
    if value is None:
        return (None, None)
    try:
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            numeric_value = None
    except (TypeError, ValueError):
        numeric_value = None
    text_value = str(value)
    if len(text_value) > 256:
        text_value = text_value[:256]
    return (text_value, numeric_value)


def extract_item_value(item: dict[str, Any]) -> tuple[str | None, float | None]:
    for key in ("rawValue", "value", "formattedValue", "text"):
        if key in item and item[key] is not None:
            return coerce_value(item[key])
    return (None, None)


def parse_source_ts(item: dict[str, Any], default_ts: str) -> str:
    for key in ("timestamp", "lastTimestamp", "last_seen", "datetime"):
        value = item.get(key)
        if value is None or value == "":
            continue
        if isinstance(value, (int, float)):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(value)))
        text = str(value).strip()
        if text.isdigit():
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(text)))
        return text
    return default_ts


def resolve_installation_id(session: requests.Session) -> str:
    """Resolve gateway identifier to numeric site id for REST endpoints."""
    if str(VRM_PORTAL_ID).isdigit():
        return str(VRM_PORTAL_ID)

    me_url = f"{VRM_API_BASE}/users/me/installations"
    me_response = session.get(me_url, timeout=VRM_REQUEST_TIMEOUT)
    me_response.raise_for_status()
    me_payload = me_response.json()
    user_id = (((me_payload.get("user") or {}).get("id")))
    if user_id is None:
        raise RuntimeError("Could not determine VRM user id from /users/me/installations")

    installations_url = f"{VRM_API_BASE}/users/{user_id}/installations"
    response = session.get(installations_url, timeout=VRM_REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    for item in payload.get("records", []):
        if str(item.get("identifier")) == str(VRM_PORTAL_ID):
            site_id = item.get("idSite")
            if site_id is not None:
                return str(site_id)
    raise RuntimeError(f"Could not resolve VRM site id for identifier {VRM_PORTAL_ID}")


def extract_diagnostics_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    records = payload.get("records")
    if isinstance(records, list):
        return [item for item in records if isinstance(item, dict)]
    if isinstance(records, dict):
        data = records.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

    for key in ("diagnostics", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def map_diagnostics_payload(payload: Any, observed_at: str | None = None) -> dict[str, dict[str, Any]]:
    observed_at = observed_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mapped: dict[str, dict[str, Any]] = {}
    for item in extract_diagnostics_items(payload):
        metric = extract_metric_name(item)
        if metric is None:
            continue
        value_str, value_num = extract_item_value(item)
        if value_str is None:
            continue
        mapped[metric] = {
            "value": value_str,
            "value_num": value_num,
            "source_ts": parse_source_ts(item, observed_at),
        }
    return mapped


def store_metrics(metrics: dict[str, dict[str, Any]]) -> None:
    if not metrics:
        return
    with buffer_lock:
        metric_buffer.update(metrics)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS victron_latest (
    metric TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    value_num DOUBLE PRECISION,
    source_ts TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS victron_history (
    id BIGSERIAL PRIMARY KEY,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    source_ts TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_victron_history_metric_time
    ON victron_history(metric, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_victron_history_time
    ON victron_history(recorded_at);
"""


def db_conninfo() -> str:
    return (
        f"host={DATABASE_HOST} port={DATABASE_PORT} "
        f"dbname={DATABASE_NAME} user={DATABASE_USER} password={DATABASE_PASS}"
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Database schema ensured")


def flush_to_db(conn: psycopg.Connection) -> int:
    with buffer_lock:
        snapshot = dict(metric_buffer)
        metric_buffer.clear()

    if not snapshot:
        return 0

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with conn.cursor() as cur:
        for metric, data in snapshot.items():
            cur.execute(
                """INSERT INTO victron_latest (metric, value, value_num, source_ts, recorded_at)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (metric) DO UPDATE SET
                     value = EXCLUDED.value,
                     value_num = EXCLUDED.value_num,
                     source_ts = EXCLUDED.source_ts,
                     recorded_at = EXCLUDED.recorded_at""",
                (metric, data["value"], data["value_num"], data.get("source_ts"), now),
            )
            if data["value_num"] is not None:
                cur.execute(
                    """INSERT INTO victron_history (metric, value, source_ts, recorded_at)
                       VALUES (%s, %s, %s, %s)""",
                    (metric, data["value_num"], data.get("source_ts"), now),
                )
    conn.commit()
    count = len(snapshot)
    log.debug("Flushed %d metrics to Postgres", count)
    return count


def db_writer_loop() -> None:
    conn = psycopg.connect(db_conninfo(), autocommit=False)
    ensure_schema(conn)

    while not shutdown_event.is_set():
        try:
            flush_to_db(conn)
        except psycopg.OperationalError:
            log.exception("Postgres connection lost, reconnecting")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(5)
            conn = psycopg.connect(db_conninfo(), autocommit=False)
        except Exception:
            log.exception("Unexpected error in DB writer")

        shutdown_event.wait(timeout=DB_WRITE_INTERVAL)

    try:
        flush_to_db(conn)
        conn.close()
    except Exception:
        pass
    log.info("DB writer stopped")


# ---------------------------------------------------------------------------
# Local MQTT (Mosquitto republish)
# ---------------------------------------------------------------------------

local_client: mqtt.Client | None = None


def connect_local_mqtt() -> mqtt.Client:
    client = mqtt.Client(
        client_id="vrm-bridge-local",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )

    if LOCAL_MQTT_USERNAME:
        client.username_pw_set(LOCAL_MQTT_USERNAME, LOCAL_MQTT_PASSWORD or None)

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            log.info("Connected to local MQTT at %s:%d", LOCAL_MQTT_HOST, LOCAL_MQTT_PORT)
        else:
            log.warning("Local MQTT connect failed: rc=%s", rc)

    client.on_connect = on_connect
    client.connect_async(LOCAL_MQTT_HOST, LOCAL_MQTT_PORT)
    client.loop_start()
    return client


def republish_local(metric: str, value_str: str) -> None:
    if local_client and local_client.is_connected():
        topic = f"{LOCAL_MQTT_PREFIX}/{metric}"
        local_client.publish(topic, value_str, qos=0, retain=True)


def push_lakemates(metrics: dict[str, dict[str, Any]], captured_at: str) -> None:
    if not LAKEMATES_PUSH_URL or not LAKEMATES_SITE_KEY or not metrics:
        return

    if not LAKEMATES_PUSH_URL.startswith("https://"):
        raise RuntimeError("LAKEMATES_PUSH_URL must use https")

    if not LAKEMATES_INGEST_SECRET:
        raise RuntimeError("LAKEMATES_INGEST_SECRET is required when pushing to Lakemates")

    payload = {
        "siteKey": LAKEMATES_SITE_KEY,
        "capturedAt": captured_at,
        "metrics": [
            {
                "metricKey": metric,
                "metricValue": data.get("value"),
                "metricValueNum": data.get("value_num"),
                "sourceTs": data.get("source_ts") or captured_at,
            }
            for metric, data in metrics.items()
        ],
    }

    response = requests.post(
        LAKEMATES_PUSH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Ingest-Secret": LAKEMATES_INGEST_SECRET,
        },
        data=json.dumps(payload),
        timeout=LAKEMATES_PUSH_TIMEOUT,
    )
    response.raise_for_status()


# ---------------------------------------------------------------------------
# VRM REST polling
# ---------------------------------------------------------------------------

def poll_vrm(session: requests.Session, installation_id: str) -> dict[str, dict[str, Any]]:
    url = f"{VRM_API_BASE}/installations/{installation_id}/diagnostics"
    response = session.get(url, timeout=VRM_REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    observed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    metrics = map_diagnostics_payload(payload, observed_at=observed_at)
    log.info("Fetched %d metrics from VRM API", len(metrics))
    if metrics:
        HEALTH_FILE.touch()
    return metrics


def poller_loop() -> None:
    session = requests.Session()
    session.headers.update({"X-Authorization": f"Token {VRM_TOKEN}"})
    installation_id = resolve_installation_id(session)
    log.info("Resolved VRM installation %s -> site id %s", VRM_PORTAL_ID, installation_id)

    while not shutdown_event.is_set():
        try:
            metrics = poll_vrm(session, installation_id)
            observed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            store_metrics(metrics)
            for metric, data in metrics.items():
                republish_local(metric, data["value"])
            if LAKEMATES_PUSH_URL and LAKEMATES_SITE_KEY:
                push_lakemates(metrics, observed_at)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            body = exc.response.text[:500] if exc.response is not None else ""
            log.error("VRM API request failed: status=%s body=%s", status, body)
        except requests.RequestException:
            log.exception("VRM API request failed")
        except Exception:
            log.exception("Unexpected error while polling VRM API")

        shutdown_event.wait(timeout=VRM_POLL_INTERVAL)

    session.close()
    log.info("VRM poller stopped")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("VRM REST Bridge starting (portal=%s, db=%s/%s)", VRM_PORTAL_ID, DATABASE_HOST, DATABASE_NAME)

    global local_client
    local_client = connect_local_mqtt()

    db_thread = threading.Thread(target=db_writer_loop, name="db-writer", daemon=True)
    db_thread.start()

    poll_thread = threading.Thread(target=poller_loop, name="vrm-poller", daemon=True)
    poll_thread.start()

    def handle_signal(signum, frame):
        log.info("Received signal %s, shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info("Bridge running. Press Ctrl+C to stop.")
    shutdown_event.wait()

    if local_client:
        local_client.loop_stop()
        local_client.disconnect()

    poll_thread.join(timeout=10)
    db_thread.join(timeout=10)
    HEALTH_FILE.unlink(missing_ok=True)
    log.info("Bridge stopped cleanly")


if __name__ == "__main__":
    main()

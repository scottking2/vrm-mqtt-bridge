"""Victron VRM bridge with stage-safe multi-boat runtime support."""

import base64
import hashlib
import json
import logging
import math
import os
import signal
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt
import psycopg
import requests

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:  # pragma: no cover - exercised in runtime env validation
    AESGCM = None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("vrm-bridge")

HEALTH_FILE = Path("/tmp/healthy")

# ---------------------------------------------------------------------------
# Metric mapping and metadata
# ---------------------------------------------------------------------------

METRIC_MAP: dict[str, str] = {
    # battery
    "battery_soc": "battery_soc",
    "battery_voltage": "battery_voltage",
    "battery_current": "battery_current",
    "battery_power": "battery_power",
    "battery_temperature": "battery_temperature",
    "battery_time_to_go": "battery_time_to_go",
    "Dc/Battery/Soc": "battery_soc",
    "Dc/Battery/Voltage": "battery_voltage",
    "Dc/Battery/Current": "battery_current",
    "Dc/Battery/Power": "battery_power",
    "Dc/Battery/Temperature": "battery_temperature",
    "Dc/Battery/TimeToGo": "battery_time_to_go",
    "battery/Soc": "battery_soc",
    "battery/Dc/0/Voltage": "battery_voltage",
    "battery/Dc/0/Current": "battery_current",
    "battery/Dc/0/Temperature": "battery_temperature",
    "system/Dc/Battery/Power": "battery_power",
    "system/Dc/Battery/TimeToGo": "battery_time_to_go",
    "/Soc": "battery_soc",
    "/Dc/0/Voltage": "battery_voltage",
    "/Dc/0/Current": "battery_current",
    "/Dc/0/Temperature": "battery_temperature",
    "/Dc/Battery/Power": "battery_power",
    "/Dc/Battery/TimeToGo": "battery_time_to_go",

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
    "ac_output_l1": "ac_output_l1",
    "system_state": "system_state",
    "Ac/Consumption/L1/Power": "ac_consumption_l1",
    "Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "Ac/Out/L1/P": "ac_output_l1",
    "SystemState/State": "system_state",
    "system/Ac/Consumption/L1/Power": "ac_consumption_l1",
    "system/Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "system/Ac/Out/L1/P": "ac_output_l1",
    "system/SystemState/State": "system_state",
    "/Ac/Consumption/L1/Power": "ac_consumption_l1",
    "/Ac/ConsumptionOnOutput/L1/Power": "ac_loads_on_output_l1",
    "/Ac/Out/L1/P": "ac_output_l1",
    "/SystemState/State": "system_state",

    # source / inverter
    "ac_active_in_l1": "ac_active_in_l1",
    "Ac/ActiveIn/L1/P": "ac_active_in_l1",
    "vebus/Ac/ActiveIn/L1/P": "ac_active_in_l1",
    "/Ac/ActiveIn/L1/P": "ac_active_in_l1",
}

METRIC_DEFINITIONS: dict[str, dict[str, Any]] = {
    "battery_soc": {"group": "battery", "unit": "percent", "summary_key": "batterySoc"},
    "battery_voltage": {"group": "battery", "unit": "volts", "summary_key": "batteryVoltage"},
    "battery_current": {"group": "battery", "unit": "amps", "summary_key": "batteryCurrent"},
    "battery_power": {"group": "battery", "unit": "watts", "summary_key": "batteryPower"},
    "battery_temperature": {"group": "battery", "unit": "celsius"},
    "battery_time_to_go": {"group": "battery", "unit": "seconds"},
    "solar_power": {"group": "solar", "unit": "watts", "summary_key": "solarPower"},
    "solar_current": {"group": "solar", "unit": "amps", "summary_key": "solarCurrent"},
    "ac_consumption_l1": {"group": "ac", "unit": "watts", "summary_key": "acConsumptionL1"},
    "ac_loads_on_output_l1": {"group": "ac", "unit": "watts", "summary_key": "acLoadsOnOutputL1"},
    "ac_output_l1": {"group": "ac", "unit": "watts", "summary_key": "acOutputL1"},
    "ac_active_in_l1": {"group": "ac", "unit": "watts", "summary_key": "acActiveInL1"},
    "system_state": {"group": "system", "unit": "state", "summary_key": "systemState"},
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def first_present(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return default


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    name: str
    user: str
    password: str
    port: int = 5432


@dataclass(frozen=True)
class MqttConfig:
    host: str = "localhost"
    port: int = 1883
    prefix: str = "victron"
    username: str = ""
    password: str = ""
    enabled: bool = True


@dataclass(frozen=True)
class LakematesPushConfig:
    push_url: str = ""
    site_key: str = ""
    ingest_secret: str = ""
    timeout: int = 10
    include_summary: bool = False


@dataclass(frozen=True)
class LakematesInternalConfig:
    enabled: bool = False
    base_url: str = ""
    machine_token: str = ""
    encryption_key: str = ""
    config_path: str = "/api/internal/victron/integrations"
    ingest_path: str = "/api/internal/victron/ingest"
    status_path: str = "/api/internal/victron/integrations/status"
    request_timeout: int = 15
    refresh_interval: int = 300
    allow_static_fallback: bool = True


@dataclass(frozen=True)
class IntegrationConfig:
    boat_slug: str
    boat_name: str
    enabled: bool
    vrm_token: str
    vrm_portal_id: str
    vrm_api_base: str
    poll_interval: int
    request_timeout: int
    lakemates: LakematesPushConfig
    source: str = "static"
    config_version: str = ""


@dataclass(frozen=True)
class BridgeConfig:
    database: DatabaseConfig
    mqtt: MqttConfig
    db_write_interval: int
    integrations: tuple[IntegrationConfig, ...]
    lakemates_internal: LakematesInternalConfig = field(default_factory=LakematesInternalConfig)


def _require_str(mapping: dict[str, Any], *keys: str) -> str:
    value = first_present(mapping, *keys)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"Missing required config field: {keys[0]}")
    return str(value).strip()


def _parse_int(value: Any, *, field_name: str, default: int | None = None) -> int:
    if value is None:
        if default is None:
            raise RuntimeError(f"Missing required integer config field: {field_name}")
        return default
    return int(value)


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_structured_payload() -> dict[str, Any] | None:
    config_file = os.environ.get("BRIDGE_CONFIG_FILE", "").strip()
    config_json = os.environ.get("BRIDGE_CONFIG_JSON", "").strip()

    if config_file:
        return json.loads(Path(config_file).read_text())
    if config_json:
        return json.loads(config_json)
    return None


def _load_database_config(payload: dict[str, Any]) -> DatabaseConfig:
    database = payload.get("database") or {}
    if not isinstance(database, dict):
        raise RuntimeError("database config must be an object")
    return DatabaseConfig(
        host=_require_str(database, "host"),
        name=_require_str(database, "name", "dbname"),
        user=_require_str(database, "user", "username"),
        password=_require_str(database, "password"),
        port=_parse_int(first_present(database, "port"), field_name="database.port", default=5432),
    )


def _load_mqtt_config(payload: dict[str, Any]) -> MqttConfig:
    mqtt_payload = payload.get("mqtt") or {}
    if not isinstance(mqtt_payload, dict):
        raise RuntimeError("mqtt config must be an object")
    return MqttConfig(
        host=str(first_present(mqtt_payload, "host", default="localhost")),
        port=_parse_int(first_present(mqtt_payload, "port"), field_name="mqtt.port", default=1883),
        prefix=str(first_present(mqtt_payload, "prefix", default="victron")).strip("/") or "victron",
        username=str(first_present(mqtt_payload, "username", default="")).strip(),
        password=str(first_present(mqtt_payload, "password", default="")).strip(),
        enabled=_parse_bool(first_present(mqtt_payload, "enabled"), default=True),
    )


def _load_lakemates_push_config(item: dict[str, Any], defaults: dict[str, Any]) -> LakematesPushConfig:
    lakemates = item.get("lakemates") or {}
    if not isinstance(lakemates, dict):
        raise RuntimeError("integration.lakemates config must be an object")
    return LakematesPushConfig(
        push_url=str(first_present(lakemates, "pushUrl", "push_url", default=first_present(defaults, "lakematesPushUrl", "pushUrl", default=""))).strip(),
        site_key=str(first_present(lakemates, "siteKey", "site_key", default=first_present(item, "lakematesSiteKey", "siteKey", default=""))).strip(),
        ingest_secret=str(first_present(lakemates, "ingestSecret", "ingest_secret", default=first_present(defaults, "lakematesIngestSecret", default=""))).strip(),
        timeout=_parse_int(
            first_present(lakemates, "timeout", "pushTimeout", default=first_present(defaults, "lakematesPushTimeout")),
            field_name="integration.lakemates.timeout",
            default=10,
        ),
        include_summary=_parse_bool(
            first_present(lakemates, "includeSummary", default=first_present(defaults, "lakematesIncludeSummary")),
            default=False,
        ),
    )


def _extract_plain_credentials(item: dict[str, Any]) -> dict[str, Any]:
    credentials = first_present(item, "credentials", "victronCredentials", default={})
    if isinstance(credentials, dict):
        return credentials
    return {}


def _extract_encrypted_credentials_payload(item: dict[str, Any]) -> dict[str, Any] | None:
    for key in (
        "encryptedCredentials",
        "credentialsEncrypted",
        "encryptedCredentialPayload",
        "victronEncryptedCredentials",
    ):
        payload = item.get(key)
        if isinstance(payload, dict):
            return payload
    return None


def _load_integration_config(
    item: dict[str, Any],
    defaults: dict[str, Any],
    *,
    require_credentials: bool = True,
    vrm_token: str | None = None,
    source: str = "static",
) -> IntegrationConfig:
    if not isinstance(item, dict):
        raise RuntimeError("integration entries must be objects")

    boat_slug = _require_str(item, "boatSlug", "boatKey", "tenantSlug", "slug")
    boat_name = str(first_present(item, "boatName", "displayName", "name", default=boat_slug)).strip() or boat_slug
    enabled = _parse_bool(first_present(item, "enabled"), default=True)
    resolved_token = vrm_token if vrm_token is not None else first_present(item, "vrmToken", "token")
    resolved_portal_id = first_present(item, "vrmPortalId", "portalId")

    if require_credentials or enabled:
        if resolved_token is None or str(resolved_token).strip() == "":
            raise RuntimeError(f"Missing required config field: {boat_slug}.vrmToken")
        if resolved_portal_id is None or str(resolved_portal_id).strip() == "":
            raise RuntimeError(f"Missing required config field: {boat_slug}.vrmPortalId")

    return IntegrationConfig(
        boat_slug=boat_slug,
        boat_name=boat_name,
        enabled=enabled,
        vrm_token=str(resolved_token or "").strip(),
        vrm_portal_id=str(resolved_portal_id or "").strip(),
        vrm_api_base=str(first_present(item, "vrmApiBase", default=first_present(defaults, "vrmApiBase", default="https://vrmapi.victronenergy.com/v2"))).rstrip("/"),
        poll_interval=_parse_int(first_present(item, "pollInterval", default=first_present(defaults, "pollInterval")), field_name=f"{boat_slug}.pollInterval", default=30),
        request_timeout=_parse_int(first_present(item, "requestTimeout", default=first_present(defaults, "requestTimeout")), field_name=f"{boat_slug}.requestTimeout", default=20),
        lakemates=_load_lakemates_push_config(item, defaults),
        source=source,
        config_version=str(first_present(item, "configVersion", "version", default="")).strip(),
    )


def _load_lakemates_internal_config(payload: dict[str, Any] | None = None) -> LakematesInternalConfig:
    block = (payload or {}).get("lakematesInternal") or {}
    if block and not isinstance(block, dict):
        raise RuntimeError("lakematesInternal config must be an object")

    base_url = str(first_present(block, "baseUrl", default=os.environ.get("VICTRON_INTERNAL_API_BASE_URL", ""))).rstrip("/")
    machine_token = str(first_present(block, "machineToken", default=os.environ.get("VICTRON_BRIDGE_MACHINE_TOKEN", ""))).strip()
    encryption_key = str(first_present(block, "encryptionKey", default=os.environ.get("VICTRON_ENCRYPTION_KEY", ""))).strip()
    enabled = _parse_bool(
        first_present(block, "enabled", default=os.environ.get("VICTRON_CONFIG_PULL_ENABLED")),
        default=bool(base_url and machine_token and encryption_key),
    )

    return LakematesInternalConfig(
        enabled=enabled,
        base_url=base_url,
        machine_token=machine_token,
        encryption_key=encryption_key,
        config_path=str(first_present(block, "configPath", default=os.environ.get("VICTRON_INTERNAL_API_CONFIG_PATH", "/api/internal/victron/integrations"))).strip() or "/api/internal/victron/integrations",
        ingest_path=str(first_present(block, "ingestPath", default=os.environ.get("VICTRON_INTERNAL_API_INGEST_PATH", "/api/internal/victron/ingest"))).strip() or "/api/internal/victron/ingest",
        status_path=str(first_present(block, "statusPath", default=os.environ.get("VICTRON_INTERNAL_API_STATUS_PATH", "/api/internal/victron/integrations/status"))).strip() or "/api/internal/victron/integrations/status",
        request_timeout=_parse_int(
            first_present(block, "requestTimeout", default=os.environ.get("VICTRON_INTERNAL_API_TIMEOUT")),
            field_name="lakematesInternal.requestTimeout",
            default=15,
        ),
        refresh_interval=_parse_int(
            first_present(block, "refreshInterval", default=os.environ.get("VICTRON_CONFIG_REFRESH_INTERVAL")),
            field_name="lakematesInternal.refreshInterval",
            default=300,
        ),
        allow_static_fallback=_parse_bool(
            first_present(block, "allowStaticFallback", default=os.environ.get("VICTRON_CONFIG_PULL_ALLOW_STATIC_FALLBACK")),
            default=False,
        ),
    )


def _legacy_bridge_config(machine_config: LakematesInternalConfig) -> BridgeConfig:
    legacy_integrations: tuple[IntegrationConfig, ...] = ()
    if os.environ.get("VRM_TOKEN", "").strip() and os.environ.get("VRM_PORTAL_ID", "").strip():
        legacy_integrations = (
            IntegrationConfig(
                boat_slug=os.environ.get("BOAT_SLUG", "default").strip() or "default",
                boat_name=os.environ.get("BOAT_NAME", os.environ.get("BOAT_SLUG", "default")).strip() or "default",
                enabled=env_bool("INTEGRATION_ENABLED", default=True),
                vrm_token=os.environ["VRM_TOKEN"],
                vrm_portal_id=os.environ["VRM_PORTAL_ID"],
                vrm_api_base=os.environ.get("VRM_API_BASE", "https://vrmapi.victronenergy.com/v2").rstrip("/"),
                poll_interval=int(os.environ.get("VRM_POLL_INTERVAL", os.environ.get("DB_WRITE_INTERVAL", "30"))),
                request_timeout=int(os.environ.get("VRM_REQUEST_TIMEOUT", "20")),
                lakemates=LakematesPushConfig(
                    push_url=os.environ.get("LAKEMATES_PUSH_URL", "").strip(),
                    site_key=os.environ.get("LAKEMATES_SITE_KEY", "").strip(),
                    ingest_secret=os.environ.get("LAKEMATES_INGEST_SECRET", "").strip(),
                    timeout=int(os.environ.get("LAKEMATES_PUSH_TIMEOUT", "10")),
                    include_summary=env_bool("LAKEMATES_INCLUDE_SUMMARY", default=False),
                ),
                source="legacy-env",
            ),
        )
    elif not machine_config.enabled:
        raise RuntimeError("Legacy config requires VRM_TOKEN and VRM_PORTAL_ID when Lakemates config-pull is disabled")

    return BridgeConfig(
        database=DatabaseConfig(
            host=os.environ["DATABASE_HOST"],
            name=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USER"],
            password=os.environ["DATABASE_PASS"],
            port=int(os.environ.get("DATABASE_PORT", "5432")),
        ),
        mqtt=MqttConfig(
            host=os.environ.get("LOCAL_MQTT_HOST", "localhost"),
            port=int(os.environ.get("LOCAL_MQTT_PORT", "1883")),
            prefix=os.environ.get("LOCAL_MQTT_PREFIX", "victron").strip("/") or "victron",
            username=os.environ.get("LOCAL_MQTT_USERNAME", "").strip(),
            password=os.environ.get("LOCAL_MQTT_PASSWORD", "").strip(),
            enabled=env_bool("LOCAL_MQTT_ENABLED", default=True),
        ),
        db_write_interval=int(os.environ.get("DB_WRITE_INTERVAL", "30")),
        integrations=legacy_integrations,
        lakemates_internal=machine_config,
    )


def load_bridge_config() -> BridgeConfig:
    payload = _load_structured_payload()
    if payload is not None:
        if not isinstance(payload, dict):
            raise RuntimeError("Bridge config payload must be a JSON object")
        defaults = payload.get("defaults") or {}
        if not isinstance(defaults, dict):
            raise RuntimeError("defaults config must be an object")
        machine_config = _load_lakemates_internal_config(payload)
        integrations_payload = payload.get("integrations") or []
        if not isinstance(integrations_payload, list):
            raise RuntimeError("Bridge config integrations must be an array")
        integrations = tuple(_load_integration_config(item, defaults) for item in integrations_payload)
        if not integrations and not machine_config.enabled:
            raise RuntimeError("Bridge config must define at least one integration")
        return BridgeConfig(
            database=_load_database_config(payload),
            mqtt=_load_mqtt_config(payload),
            db_write_interval=_parse_int(first_present(payload, "dbWriteInterval"), field_name="dbWriteInterval", default=30),
            integrations=integrations,
            lakemates_internal=machine_config,
        )

    machine_config = _load_lakemates_internal_config()
    return _legacy_bridge_config(machine_config)


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


def metric_metadata(metric: str) -> dict[str, Any]:
    return METRIC_DEFINITIONS.get(metric, {"group": "other", "unit": None})


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


def build_metric_record(metric: str, value_str: str, value_num: float | None, source_ts: str) -> dict[str, Any]:
    metadata = metric_metadata(metric)
    return {
        "value": value_str,
        "value_num": value_num,
        "source_ts": source_ts,
        "group": metadata.get("group"),
        "unit": metadata.get("unit"),
    }


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
        mapped[metric] = build_metric_record(metric, value_str, value_num, parse_source_ts(item, observed_at))
    return mapped


def build_summary(metrics: dict[str, dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for metric, data in metrics.items():
        summary_key = metric_metadata(metric).get("summary_key")
        if not summary_key:
            continue
        summary[summary_key] = data.get("value_num") if data.get("value_num") is not None else data.get("value")
    return summary


# ---------------------------------------------------------------------------
# Lakemates config pull / decrypt
# ---------------------------------------------------------------------------

def _decode_secret_bytes(value: str) -> bytes:
    text = value.strip()
    if not text:
        raise RuntimeError("Encryption key is empty")
    return hashlib.sha256(text.encode("utf-8")).digest()


def _decode_encrypted_field(value: Any, field_name: str) -> bytes:
    if value is None:
        raise RuntimeError(f"Missing encrypted payload field: {field_name}")
    text = str(value).strip()
    if not text:
        raise RuntimeError(f"Missing encrypted payload field: {field_name}")
    try:
        return base64.b64decode(text, validate=True)
    except Exception as exc:
        raise RuntimeError(f"Invalid base64 for encrypted payload field: {field_name}") from exc


def _decrypt_aes_gcm_payload_bytes(payload: dict[str, Any], encryption_key: str) -> bytes:
    algorithm = str(first_present(payload, "algorithm", default="aes-256-gcm")).strip().lower()
    if algorithm not in {"aes-256-gcm", "aes-gcm"}:
        raise RuntimeError(f"Unsupported encrypted credentials algorithm: {algorithm}")
    if AESGCM is None:
        raise RuntimeError("cryptography package is required for Lakemates encrypted credential decrypt")

    key = _decode_secret_bytes(encryption_key)
    nonce = _decode_encrypted_field(first_present(payload, "iv", "nonce"), "iv")
    ciphertext = _decode_encrypted_field(payload.get("ciphertext"), "ciphertext")
    tag = _decode_encrypted_field(first_present(payload, "tag", "authTag"), "tag")
    aad_value = first_present(payload, "aad", "additionalData")
    aad = _decode_encrypted_field(aad_value, "aad") if aad_value else None
    return AESGCM(key).decrypt(nonce, ciphertext + tag, aad)


def decrypt_json_payload(payload: dict[str, Any], encryption_key: str) -> dict[str, Any]:
    decrypted = _decrypt_aes_gcm_payload_bytes(payload, encryption_key)
    try:
        decoded_text = decrypted.decode("utf-8")
        decoded = json.loads(decoded_text)
    except json.JSONDecodeError:
        return {"vrmToken": decoded_text}
    finally:
        decrypted = b""
    if isinstance(decoded, str):
        return {"vrmToken": decoded}
    if not isinstance(decoded, dict):
        raise RuntimeError("Decrypted credentials payload must be a JSON object")
    return decoded


def load_integration_from_lakemates(
    item: dict[str, Any],
    defaults: dict[str, Any],
    machine_config: LakematesInternalConfig,
) -> IntegrationConfig:
    credentials = _extract_plain_credentials(item)
    encrypted_payload = _extract_encrypted_credentials_payload(item)
    if encrypted_payload:
        credentials = decrypt_json_payload(encrypted_payload, machine_config.encryption_key)

    vrm_token = str(
        first_present(
            credentials,
            "vrmToken",
            "token",
            default=first_present(item, "vrmToken", "token", default=""),
        )
    ).strip()
    portal_id = str(
        first_present(
            item,
            "vrmPortalId",
            "portalId",
            default=first_present(credentials, "vrmPortalId", "portalId", default=""),
        )
    ).strip()

    runtime_item = dict(item)
    runtime_item["vrmPortalId"] = portal_id
    return _load_integration_config(
        runtime_item,
        defaults,
        require_credentials=False,
        vrm_token=vrm_token,
        source="lakemates-api",
    )


class LakematesInternalApiClient:
    def __init__(self, config: LakematesInternalConfig, session: requests.Session | None = None):
        if not config.enabled:
            raise RuntimeError("Lakemates internal API client requires config-pull to be enabled")
        if not config.base_url or not config.machine_token or not config.encryption_key:
            raise RuntimeError("Lakemates config-pull requires base URL, machine token, and encryption key")
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.machine_token}",
                "Content-Type": "application/json",
                "User-Agent": "vrm-mqtt-bridge/2",
            }
        )

    def build_url(self, path: str) -> str:
        return f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"

    def fetch_integrations(self) -> tuple[IntegrationConfig, ...]:
        response = self.session.get(
            self.build_url(self.config.config_path),
            timeout=self.config.request_timeout,
        )
        response.raise_for_status()
        payload = response.json()
        defaults: dict[str, Any] = {}
        integrations_payload: list[dict[str, Any]] = []

        if isinstance(payload, list):
            integrations_payload = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            defaults_candidate = payload.get("defaults") or {}
            if isinstance(defaults_candidate, dict):
                defaults = defaults_candidate
            integrations_candidate = first_present(payload, "integrations", "items", default=[])
            if isinstance(integrations_candidate, list):
                integrations_payload = [item for item in integrations_candidate if isinstance(item, dict)]
        else:
            raise RuntimeError("Lakemates config response must be a JSON object or array")

        integrations = tuple(
            load_integration_from_lakemates(item, defaults, self.config)
            for item in integrations_payload
        )
        log.info("Loaded %d integration(s) from Lakemates config API", len(integrations))
        return integrations

    def post_status(self, status: "IntegrationStatus") -> None:
        if not self.config.status_path:
            return
        payload = {
            "boatKey": status.boat_slug,
            "status": map_runtime_status(status.runtime_status),
            "lastSyncAt": status.last_poll_at,
            "lastSnapshotAt": status.last_push_at,
            "lastSuccessAt": status.last_success_at,
            "lastErrorAt": status.last_error_at,
            "lastErrorCode": status.last_error_code,
            "lastErrorMessage": status.last_error_message,
            "eventType": "bridge_heartbeat",
            "eventDetail": f"runtime={status.runtime_status}; metrics={status.last_metric_count}; failures={status.consecutive_failures}",
        }
        response = self.session.post(
            self.build_url(self.config.status_path),
            data=json.dumps(payload),
            timeout=self.config.request_timeout,
        )
        response.raise_for_status()

    def close(self) -> None:
        self.session.close()


# ---------------------------------------------------------------------------
# VRM polling
# ---------------------------------------------------------------------------

shutdown_event = threading.Event()


def resolve_installation_id(session: requests.Session, integration: IntegrationConfig) -> str:
    """Resolve gateway identifier to numeric site id for REST endpoints."""
    if str(integration.vrm_portal_id).isdigit():
        return str(integration.vrm_portal_id)

    me_url = f"{integration.vrm_api_base}/users/me/installations"
    me_response = session.get(me_url, timeout=integration.request_timeout)
    me_response.raise_for_status()
    me_payload = me_response.json()
    user_id = (((me_payload.get("user") or {}).get("id")))
    if user_id is None:
        raise RuntimeError("Could not determine VRM user id from /users/me/installations")

    installations_url = f"{integration.vrm_api_base}/users/{user_id}/installations"
    response = session.get(installations_url, timeout=integration.request_timeout)
    response.raise_for_status()
    payload = response.json()
    for item in payload.get("records", []):
        if str(item.get("identifier")) == str(integration.vrm_portal_id):
            site_id = item.get("idSite")
            if site_id is not None:
                return str(site_id)
    raise RuntimeError(f"Could not resolve VRM site id for identifier {integration.vrm_portal_id}")


def poll_vrm(session: requests.Session, integration: IntegrationConfig, installation_id: str) -> dict[str, dict[str, Any]]:
    url = f"{integration.vrm_api_base}/installations/{installation_id}/diagnostics"
    response = session.get(url, timeout=integration.request_timeout)
    response.raise_for_status()
    payload = response.json()
    observed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    metrics = map_diagnostics_payload(payload, observed_at=observed_at)
    log.info("[%s] fetched %d metrics from VRM API", integration.boat_slug, len(metrics))
    if metrics:
        HEALTH_FILE.touch()
    return metrics


def describe_http_error(exc: requests.HTTPError, prefix: str) -> tuple[str, str]:
    status = str(exc.response.status_code) if exc.response is not None else "http_error"
    reason = exc.response.reason if exc.response is not None and exc.response.reason else prefix
    return status, f"{prefix}: HTTP {status} {reason}".strip()


def map_runtime_status(runtime_status: str) -> str:
    normalized = (runtime_status or "").strip().lower()
    if normalized in {"healthy", "success"}:
        return "healthy"
    if normalized in {"disabled"}:
        return "disabled"
    if normalized in {"suspended"}:
        return "suspended"
    if normalized in {"missing_credentials", "pending_credentials"}:
        return "pending_credentials"
    if normalized in {"starting", "polling", "pending_sync"}:
        return "pending_sync"
    return "error"


def publish_status_callback(client: LakematesInternalApiClient | None, state: "BridgeState", boat_slug: str) -> None:
    if client is None:
        return
    status = state.get_status(boat_slug)
    if status is None:
        return
    try:
        client.post_status(status)
    except requests.HTTPError as exc:
        http_status = exc.response.status_code if exc.response is not None else "unknown"
        log.warning("[%s] Lakemates status callback failed: HTTP %s", boat_slug, http_status)
    except requests.RequestException:
        log.warning("[%s] Lakemates status callback failed: request exception", boat_slug)
    except Exception:
        log.warning("[%s] Lakemates status callback failed: unexpected error", boat_slug)


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

@dataclass
class IntegrationStatus:
    boat_slug: str
    boat_name: str
    site_key: str
    enabled: bool
    runtime_status: str
    last_poll_at: str | None = None
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error_message: str | None = None
    last_error_code: str | None = None
    consecutive_failures: int = 0
    last_metric_count: int = 0
    last_push_at: str | None = None


class BridgeState:
    def __init__(self, integrations: tuple[IntegrationConfig, ...]):
        self.buffer_lock = threading.Lock()
        self.status_lock = threading.Lock()
        self.metric_buffer: dict[str, dict[str, dict[str, Any]]] = {}
        self.statuses: dict[str, IntegrationStatus] = {
            integration.boat_slug: IntegrationStatus(
                boat_slug=integration.boat_slug,
                boat_name=integration.boat_name,
                site_key=integration.lakemates.site_key,
                enabled=integration.enabled,
                runtime_status="disabled" if not integration.enabled else "starting",
            )
            for integration in integrations
        }

    def store_metrics(self, boat_slug: str, metrics: dict[str, dict[str, Any]]) -> None:
        if not metrics:
            return
        with self.buffer_lock:
            self.metric_buffer.setdefault(boat_slug, {}).update(metrics)

    def pop_metric_snapshot(self) -> dict[str, dict[str, dict[str, Any]]]:
        with self.buffer_lock:
            snapshot = {
                boat_slug: dict(metrics)
                for boat_slug, metrics in self.metric_buffer.items()
                if metrics
            }
            self.metric_buffer.clear()
        return snapshot

    def mark_status(
        self,
        integration: IntegrationConfig,
        *,
        runtime_status: str,
        last_poll_at: str | None = None,
        last_success_at: str | None = None,
        last_error_at: str | None = None,
        last_error_message: str | None = None,
        last_error_code: str | None = None,
        consecutive_failures: int | None = None,
        last_metric_count: int | None = None,
        last_push_at: str | None = None,
    ) -> None:
        with self.status_lock:
            status = self.statuses.setdefault(
                integration.boat_slug,
                IntegrationStatus(
                    boat_slug=integration.boat_slug,
                    boat_name=integration.boat_name,
                    site_key=integration.lakemates.site_key,
                    enabled=integration.enabled,
                    runtime_status=runtime_status,
                ),
            )
            status.boat_name = integration.boat_name
            status.site_key = integration.lakemates.site_key
            status.enabled = integration.enabled
            status.runtime_status = runtime_status
            if last_poll_at is not None:
                status.last_poll_at = last_poll_at
            if last_success_at is not None:
                status.last_success_at = last_success_at
            if last_error_at is not None:
                status.last_error_at = last_error_at
            if last_error_message is not None:
                status.last_error_message = last_error_message[:500]
            if last_error_code is not None:
                status.last_error_code = last_error_code[:80]
            if consecutive_failures is not None:
                status.consecutive_failures = consecutive_failures
            if last_metric_count is not None:
                status.last_metric_count = last_metric_count
            if last_push_at is not None:
                status.last_push_at = last_push_at

    def record_success(self, integration: IntegrationConfig, observed_at: str, metric_count: int, pushed: bool) -> None:
        self.mark_status(
            integration,
            runtime_status="healthy",
            last_poll_at=observed_at,
            last_success_at=observed_at,
            last_error_at="",
            last_error_message="",
            last_error_code="",
            consecutive_failures=0,
            last_metric_count=metric_count,
            last_push_at=observed_at if pushed else None,
        )

    def record_failure(self, integration: IntegrationConfig, observed_at: str, error_code: str, message: str) -> None:
        with self.status_lock:
            failures = self.statuses.get(integration.boat_slug, IntegrationStatus(
                boat_slug=integration.boat_slug,
                boat_name=integration.boat_name,
                site_key=integration.lakemates.site_key,
                enabled=integration.enabled,
                runtime_status="error",
            )).consecutive_failures + 1
        self.mark_status(
            integration,
            runtime_status="error",
            last_poll_at=observed_at,
            last_error_at=observed_at,
            last_error_message=message,
            last_error_code=error_code,
            consecutive_failures=failures,
        )

    def status_snapshot(self) -> list[IntegrationStatus]:
        with self.status_lock:
            return [IntegrationStatus(**asdict(status)) for status in self.statuses.values()]

    def get_status(self, boat_slug: str) -> IntegrationStatus | None:
        with self.status_lock:
            status = self.statuses.get(boat_slug)
            return IntegrationStatus(**asdict(status)) if status else None


class IntegrationRegistry:
    def __init__(self, integrations: tuple[IntegrationConfig, ...]):
        self._lock = threading.Lock()
        self._integrations = {integration.boat_slug: integration for integration in integrations}

    def snapshot(self) -> dict[str, IntegrationConfig]:
        with self._lock:
            return dict(self._integrations)

    def get(self, boat_slug: str) -> IntegrationConfig | None:
        with self._lock:
            return self._integrations.get(boat_slug)

    def replace(self, integrations: tuple[IntegrationConfig, ...]) -> None:
        with self._lock:
            self._integrations = {integration.boat_slug: integration for integration in integrations}


@dataclass
class WorkerHandle:
    integration: IntegrationConfig
    stop_event: threading.Event
    thread: threading.Thread


class WorkerManager:
    def __init__(
        self,
        state: BridgeState,
        registry: IntegrationRegistry,
        mqtt_config: MqttConfig,
        status_client: LakematesInternalApiClient | None = None,
    ):
        self.state = state
        self.registry = registry
        self.mqtt_config = mqtt_config
        self.status_client = status_client
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerHandle] = {}

    def reconcile(self, integrations: tuple[IntegrationConfig, ...]) -> None:
        desired = {integration.boat_slug: integration for integration in integrations}

        with self._lock:
            current_slugs = set(self._workers)
            desired_slugs = set(desired)

            for boat_slug in current_slugs - desired_slugs:
                handle = self._workers.pop(boat_slug)
                handle.stop_event.set()
                removed = handle.integration
                self.state.mark_status(
                    IntegrationConfig(
                        boat_slug=removed.boat_slug,
                        boat_name=removed.boat_name,
                        enabled=False,
                        vrm_token="",
                        vrm_portal_id=removed.vrm_portal_id,
                        vrm_api_base=removed.vrm_api_base,
                        poll_interval=removed.poll_interval,
                        request_timeout=removed.request_timeout,
                        lakemates=removed.lakemates,
                        source=removed.source,
                        config_version=removed.config_version,
                    ),
                    runtime_status="disabled",
                    consecutive_failures=0,
                    last_metric_count=0,
                )

            for integration in integrations:
                existing = self._workers.get(integration.boat_slug)
                if existing and existing.integration == integration:
                    continue

                if existing:
                    existing.stop_event.set()
                    self._workers.pop(integration.boat_slug, None)

                if not integration.enabled:
                    self.state.mark_status(integration, runtime_status="disabled", consecutive_failures=0, last_metric_count=0)
                    publish_status_callback(self.status_client, self.state, integration.boat_slug)
                    continue

                stop_event = threading.Event()
                worker = threading.Thread(
                    target=integration_worker,
                    args=(integration, self.state, self.mqtt_config, stop_event, self.status_client),
                    name=f"vrm-{integration.boat_slug}",
                    daemon=True,
                )
                worker.start()
                self._workers[integration.boat_slug] = WorkerHandle(
                    integration=integration,
                    stop_event=stop_event,
                    thread=worker,
                )

            self.registry.replace(integrations)

    def stop_all(self) -> None:
        with self._lock:
            handles = list(self._workers.values())
            self._workers.clear()

        for handle in handles:
            handle.stop_event.set()
        for handle in handles:
            handle.thread.join(timeout=10)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS victron_latest (
    boat_slug TEXT NOT NULL,
    boat_name TEXT NOT NULL,
    site_key TEXT,
    metric TEXT NOT NULL,
    metric_group TEXT,
    unit TEXT,
    value TEXT NOT NULL,
    value_num DOUBLE PRECISION,
    source_ts TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (boat_slug, metric)
);

CREATE TABLE IF NOT EXISTS victron_history (
    id BIGSERIAL PRIMARY KEY,
    boat_slug TEXT NOT NULL,
    boat_name TEXT NOT NULL,
    site_key TEXT,
    metric TEXT NOT NULL,
    metric_group TEXT,
    unit TEXT,
    value_text TEXT NOT NULL,
    value_num DOUBLE PRECISION,
    source_ts TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS victron_bridge_status (
    boat_slug TEXT PRIMARY KEY,
    boat_name TEXT NOT NULL,
    site_key TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    runtime_status TEXT NOT NULL,
    last_poll_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    last_error_message TEXT,
    last_error_code TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_metric_count INTEGER NOT NULL DEFAULT 0,
    last_push_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_victron_history_boat_metric_time
    ON victron_history(boat_slug, metric, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_victron_history_boat_time
    ON victron_history(boat_slug, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_victron_status_runtime
    ON victron_bridge_status(runtime_status, updated_at DESC);
"""


def db_conninfo(database: DatabaseConfig) -> str:
    return (
        f"host={database.host} port={database.port} "
        f"dbname={database.name} user={database.user} password={database.password}"
    )


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Database schema ensured")


def flush_to_db(conn: psycopg.Connection, state: BridgeState, registry: IntegrationRegistry) -> int:
    metric_snapshot = state.pop_metric_snapshot()
    statuses = state.status_snapshot()
    status_by_slug = {status.boat_slug: status for status in statuses}
    integrations = registry.snapshot()

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    flushed_metrics = 0

    with conn.cursor() as cur:
        for boat_slug, metrics in metric_snapshot.items():
            integration = integrations.get(boat_slug)
            status = status_by_slug.get(boat_slug)
            if integration is not None:
                boat_name = integration.boat_name
                site_key = integration.lakemates.site_key or None
            elif status is not None:
                boat_name = status.boat_name
                site_key = status.site_key or None
            else:
                boat_name = boat_slug
                site_key = None

            for metric, data in metrics.items():
                cur.execute(
                    """INSERT INTO victron_latest (
                           boat_slug, boat_name, site_key, metric, metric_group, unit, value, value_num, source_ts, recorded_at
                       )
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (boat_slug, metric) DO UPDATE SET
                         boat_name = EXCLUDED.boat_name,
                         site_key = EXCLUDED.site_key,
                         metric_group = EXCLUDED.metric_group,
                         unit = EXCLUDED.unit,
                         value = EXCLUDED.value,
                         value_num = EXCLUDED.value_num,
                         source_ts = EXCLUDED.source_ts,
                         recorded_at = EXCLUDED.recorded_at""",
                    (
                        boat_slug,
                        boat_name,
                        site_key,
                        metric,
                        data.get("group"),
                        data.get("unit"),
                        data["value"],
                        data.get("value_num"),
                        data.get("source_ts"),
                        now,
                    ),
                )
                cur.execute(
                    """INSERT INTO victron_history (
                           boat_slug, boat_name, site_key, metric, metric_group, unit, value_text, value_num, source_ts, recorded_at
                       )
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        boat_slug,
                        boat_name,
                        site_key,
                        metric,
                        data.get("group"),
                        data.get("unit"),
                        data["value"],
                        data.get("value_num"),
                        data.get("source_ts"),
                        now,
                    ),
                )
                flushed_metrics += 1

        for status in statuses:
            cur.execute(
                """INSERT INTO victron_bridge_status (
                       boat_slug, boat_name, site_key, enabled, runtime_status, last_poll_at, last_success_at,
                       last_error_at, last_error_message, last_error_code, consecutive_failures,
                       last_metric_count, last_push_at, updated_at
                   )
                   VALUES (
                       %s, %s, %s, %s, %s,
                       NULLIF(%s, '')::timestamptz,
                       NULLIF(%s, '')::timestamptz,
                       NULLIF(%s, '')::timestamptz,
                       NULLIF(%s, ''),
                       NULLIF(%s, ''),
                       %s, %s,
                       NULLIF(%s, '')::timestamptz,
                       %s
                   )
                   ON CONFLICT (boat_slug) DO UPDATE SET
                     boat_name = EXCLUDED.boat_name,
                     site_key = EXCLUDED.site_key,
                     enabled = EXCLUDED.enabled,
                     runtime_status = EXCLUDED.runtime_status,
                     last_poll_at = EXCLUDED.last_poll_at,
                     last_success_at = EXCLUDED.last_success_at,
                     last_error_at = EXCLUDED.last_error_at,
                     last_error_message = EXCLUDED.last_error_message,
                     last_error_code = EXCLUDED.last_error_code,
                     consecutive_failures = EXCLUDED.consecutive_failures,
                     last_metric_count = EXCLUDED.last_metric_count,
                     last_push_at = EXCLUDED.last_push_at,
                     updated_at = EXCLUDED.updated_at""",
                (
                    status.boat_slug,
                    status.boat_name,
                    status.site_key or None,
                    status.enabled,
                    status.runtime_status,
                    status.last_poll_at or "",
                    status.last_success_at or "",
                    status.last_error_at or "",
                    status.last_error_message or "",
                    status.last_error_code or "",
                    status.consecutive_failures,
                    status.last_metric_count,
                    status.last_push_at or "",
                    now,
                ),
            )

    conn.commit()
    if flushed_metrics:
        log.debug("Flushed %d metrics to Postgres", flushed_metrics)
    return flushed_metrics


def db_writer_loop(config: BridgeConfig, state: BridgeState, registry: IntegrationRegistry) -> None:
    conn = psycopg.connect(db_conninfo(config.database), autocommit=False)
    ensure_schema(conn)

    while not shutdown_event.is_set():
        try:
            flush_to_db(conn, state, registry)
        except psycopg.OperationalError:
            log.exception("Postgres connection lost, reconnecting")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(5)
            conn = psycopg.connect(db_conninfo(config.database), autocommit=False)
        except Exception:
            log.exception("Unexpected error in DB writer")

        shutdown_event.wait(timeout=config.db_write_interval)

    try:
        flush_to_db(conn, state, registry)
        conn.close()
    except Exception:
        pass
    log.info("DB writer stopped")


# ---------------------------------------------------------------------------
# Local MQTT
# ---------------------------------------------------------------------------

local_client: mqtt.Client | None = None


def connect_local_mqtt(mqtt_config: MqttConfig) -> mqtt.Client | None:
    if not mqtt_config.enabled:
        log.info("Local MQTT disabled")
        return None

    client = mqtt.Client(
        client_id="vrm-bridge-local",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )

    if mqtt_config.username:
        client.username_pw_set(mqtt_config.username, mqtt_config.password or None)

    def on_connect(client, userdata, flags, rc, properties=None):
        del client, userdata, flags, properties
        if rc == 0:
            log.info("Connected to local MQTT at %s:%d", mqtt_config.host, mqtt_config.port)
        else:
            log.warning("Local MQTT connect failed: rc=%s", rc)

    client.on_connect = on_connect
    client.connect_async(mqtt_config.host, mqtt_config.port)
    client.loop_start()
    return client


def republish_local(mqtt_config: MqttConfig, integration: IntegrationConfig, metric: str, value_str: str) -> None:
    if local_client and local_client.is_connected():
        topic = f"{mqtt_config.prefix}/{integration.boat_slug}/{metric}"
        local_client.publish(topic, value_str, qos=0, retain=True)


# ---------------------------------------------------------------------------
# Lakemates push
# ---------------------------------------------------------------------------

def build_lakemates_payload(integration: IntegrationConfig, metrics: dict[str, dict[str, Any]], captured_at: str) -> dict[str, Any]:
    return {
        "boatKey": integration.boat_slug,
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


def push_lakemates(
    integration: IntegrationConfig,
    metrics: dict[str, dict[str, Any]],
    captured_at: str,
    status_client: LakematesInternalApiClient | None = None,
) -> bool:
    if not metrics:
        return False

    if status_client is not None:
        response = status_client.session.post(
            status_client.build_url(status_client.config.ingest_path),
            data=json.dumps(build_lakemates_payload(integration, metrics, captured_at)),
            timeout=status_client.config.request_timeout,
        )
        if response.status_code >= 400:
            log.error("[%s] Lakemates ingest failed: HTTP %s %s", integration.boat_slug, response.status_code, response.text[:500])
        response.raise_for_status()
        return True

    push = integration.lakemates
    if not push.push_url or not push.site_key:
        return False

    if not push.push_url.startswith("https://"):
        raise RuntimeError("LAKEMATES_PUSH_URL must use https")

    if not push.ingest_secret:
        raise RuntimeError("LAKEMATES_INGEST_SECRET is required when pushing to Lakemates")

    response = requests.post(
        push.push_url,
        headers={
            "Content-Type": "application/json",
            "X-Ingest-Secret": push.ingest_secret,
        },
        data=json.dumps({
            "siteKey": push.site_key,
            "capturedAt": captured_at,
            "metrics": build_lakemates_payload(integration, metrics, captured_at)["metrics"],
        }),
        timeout=push.timeout,
    )
    response.raise_for_status()
    return True


def integration_worker(
    integration: IntegrationConfig,
    state: BridgeState,
    mqtt_config: MqttConfig,
    stop_event: threading.Event,
    status_client: LakematesInternalApiClient | None = None,
) -> None:
    if not integration.enabled:
        state.mark_status(integration, runtime_status="disabled", consecutive_failures=0, last_metric_count=0)
        publish_status_callback(status_client, state, integration.boat_slug)
        log.info("[%s] integration disabled; worker not started", integration.boat_slug)
        return

    if not integration.vrm_token or not integration.vrm_portal_id:
        observed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        state.record_failure(integration, observed_at, "missing_credentials", "Integration missing VRM credentials")
        publish_status_callback(status_client, state, integration.boat_slug)
        return

    session = requests.Session()
    session.headers.update({"X-Authorization": f"Token {integration.vrm_token}"})
    installation_id: str | None = None

    while not shutdown_event.is_set() and not stop_event.is_set():
        observed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        try:
            if installation_id is None:
                installation_id = resolve_installation_id(session, integration)
                log.info("[%s] resolved portal %s -> site id %s", integration.boat_slug, integration.vrm_portal_id, installation_id)
            metrics = poll_vrm(session, integration, installation_id)
            state.store_metrics(integration.boat_slug, metrics)
            for metric, data in metrics.items():
                republish_local(mqtt_config, integration, metric, data["value"])
            pushed = push_lakemates(integration, metrics, observed_at, status_client=status_client)
            state.record_success(integration, observed_at, len(metrics), pushed)
            publish_status_callback(status_client, state, integration.boat_slug)
        except requests.HTTPError as exc:
            error_code, message = describe_http_error(exc, "VRM API request failed")
            state.record_failure(integration, observed_at, error_code, message)
            if installation_id is not None and error_code in {"401", "403", "404"}:
                installation_id = None
            publish_status_callback(status_client, state, integration.boat_slug)
            log.error("[%s] %s", integration.boat_slug, message)
        except requests.RequestException:
            state.record_failure(integration, observed_at, "request_exception", "VRM API request exception")
            publish_status_callback(status_client, state, integration.boat_slug)
            log.exception("[%s] VRM API request failed", integration.boat_slug)
        except Exception:
            error_code = "installation_resolution_failed" if installation_id is None else "unexpected_error"
            state.record_failure(integration, observed_at, error_code, "Unexpected VRM polling error")
            publish_status_callback(status_client, state, integration.boat_slug)
            log.exception("[%s] unexpected error while polling VRM API", integration.boat_slug)

        stop_event.wait(timeout=integration.poll_interval)

    session.close()
    log.info("[%s] poller stopped", integration.boat_slug)


def refresh_integrations_loop(config: BridgeConfig, manager: WorkerManager) -> None:
    client = LakematesInternalApiClient(config.lakemates_internal)
    try:
        while not shutdown_event.is_set():
            try:
                integrations = client.fetch_integrations()
                manager.reconcile(integrations)
            except requests.HTTPError as exc:
                http_status = exc.response.status_code if exc.response is not None else "unknown"
                log.warning("Lakemates config refresh failed: HTTP %s", http_status)
            except requests.RequestException:
                log.warning("Lakemates config refresh failed: request exception")
            except Exception:
                log.exception("Lakemates config refresh failed")

            shutdown_event.wait(timeout=config.lakemates_internal.refresh_interval)
    finally:
        client.close()
        log.info("Lakemates config refresh stopped")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_bridge_config()
    bootstrap_integrations = (
        config.integrations
        if (not config.lakemates_internal.enabled or config.lakemates_internal.allow_static_fallback)
        else ()
    )
    log.info(
        "VRM bridge starting with %d static integration(s); database=%s/%s; config_pull=%s",
        len(bootstrap_integrations),
        config.database.host,
        config.database.name,
        config.lakemates_internal.enabled,
    )

    state = BridgeState(bootstrap_integrations)
    registry = IntegrationRegistry(bootstrap_integrations)

    global local_client
    local_client = connect_local_mqtt(config.mqtt)

    db_thread = threading.Thread(target=db_writer_loop, args=(config, state, registry), name="db-writer", daemon=True)
    db_thread.start()

    status_client = None
    if config.lakemates_internal.enabled:
        status_client = LakematesInternalApiClient(config.lakemates_internal)

    manager = WorkerManager(state, registry, config.mqtt, status_client=status_client)

    if bootstrap_integrations:
        manager.reconcile(bootstrap_integrations)
    elif not config.lakemates_internal.enabled:
        raise RuntimeError("No integrations configured")

    refresh_thread: threading.Thread | None = None
    if config.lakemates_internal.enabled:
        refresh_thread = threading.Thread(
            target=refresh_integrations_loop,
            args=(config, manager),
            name="lakemates-refresh",
            daemon=True,
        )
        refresh_thread.start()

    def handle_signal(signum, frame):
        del frame
        log.info("Received signal %s, shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info("Bridge running. Press Ctrl+C to stop.")
    shutdown_event.wait()

    if local_client:
        local_client.loop_stop()
        local_client.disconnect()

    manager.stop_all()
    if refresh_thread is not None:
        refresh_thread.join(timeout=10)
    db_thread.join(timeout=10)
    if status_client is not None:
        status_client.close()
    HEALTH_FILE.unlink(missing_ok=True)


if __name__ == "__main__":
    main()

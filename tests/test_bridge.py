"""Unit tests for vrm bridge core logic."""

import base64
import json
import os
import sys
from unittest.mock import Mock, patch

# Inject required env vars before importing bridge
os.environ.setdefault("VRM_TOKEN", "test-token")
os.environ.setdefault("VRM_PORTAL_ID", "c0619ab12345")
os.environ.setdefault("DATABASE_HOST", "localhost")
os.environ.setdefault("DATABASE_NAME", "victron_test")
os.environ.setdefault("DATABASE_USER", "victron")
os.environ.setdefault("DATABASE_PASS", "testpass")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import bridge


class TestNormalizeMetricKey:
    def test_maps_dbus_path(self):
        assert bridge.normalize_metric_key("Dc/Battery/Soc") == "battery_soc"

    def test_maps_friendly_key(self):
        assert bridge.normalize_metric_key("battery_soc") == "battery_soc"

    def test_trims_slashes(self):
        assert bridge.normalize_metric_key("/Dc/Pv/Power/") == "solar_power"

    def test_unknown_returns_none(self):
        assert bridge.normalize_metric_key("Unknown/Path") is None


class TestCoerceValue:
    def test_numeric_value(self):
        val_str, val_num = bridge.coerce_value(85.3)
        assert val_str == "85.3"
        assert val_num == 85.3

    def test_integer_value(self):
        val_str, val_num = bridge.coerce_value(42)
        assert val_str == "42"
        assert val_num == 42.0

    def test_string_value(self):
        val_str, val_num = bridge.coerce_value("Charging")
        assert val_str == "Charging"
        assert val_num is None

    def test_none_value(self):
        val_str, val_num = bridge.coerce_value(None)
        assert val_str is None
        assert val_num is None


class TestMapDiagnosticsPayload:
    def test_maps_records_payload(self):
        payload = {
            "records": [
                {"dbusPath": "Dc/Battery/Soc", "rawValue": 84.2, "timestamp": "2026-04-11T16:00:00Z"},
                {"dbusPath": "Dc/Pv/Power", "value": 512},
                {"dbusPath": "SystemState/State", "formattedValue": "Bulk"},
            ]
        }

        mapped = bridge.map_diagnostics_payload(payload, observed_at="2026-04-11T16:05:00Z")

        assert mapped["battery_soc"] == {
            "value": "84.2",
            "value_num": 84.2,
            "source_ts": "2026-04-11T16:00:00Z",
            "group": "battery",
            "unit": "percent",
        }
        assert mapped["solar_power"] == {
            "value": "512",
            "value_num": 512.0,
            "source_ts": "2026-04-11T16:05:00Z",
            "group": "solar",
            "unit": "watts",
        }
        assert mapped["system_state"] == {
            "value": "Bulk",
            "value_num": None,
            "source_ts": "2026-04-11T16:05:00Z",
            "group": "system",
            "unit": "state",
        }

    def test_maps_list_payload_and_metric_aliases(self):
        payload = [
            {"code": "battery_power", "rawValue": -1200.5},
            {"metric": "ac_consumption_l1", "value": 900},
        ]

        mapped = bridge.map_diagnostics_payload(payload, observed_at="2026-04-11T16:05:00Z")

        assert mapped["battery_power"]["value_num"] == -1200.5
        assert mapped["ac_consumption_l1"]["value"] == "900"

    def test_skips_unknown_metrics_and_missing_values(self):
        payload = {
            "diagnostics": [
                {"dbusPath": "Unknown/Thing", "rawValue": 1},
                {"dbusPath": "Dc/Battery/Soc", "rawValue": None},
            ]
        }

        assert bridge.map_diagnostics_payload(payload) == {}

    def test_last_duplicate_metric_wins(self):
        payload = {
            "data": [
                {"dbusPath": "Dc/Pv/Power", "rawValue": 100},
                {"dbusPath": "Dc/Pv/Power", "rawValue": 200},
            ]
        }

        mapped = bridge.map_diagnostics_payload(payload)
        assert mapped["solar_power"]["value_num"] == 200.0


class TestConfigLoading:
    def test_load_bridge_config_from_structured_json(self):
        config_payload = {
            "database": {
                "host": "db",
                "name": "victron",
                "user": "bridge",
                "password": "secret",
                "port": 5433,
            },
            "mqtt": {
                "host": "mqtt",
                "port": 1884,
                "prefix": "boats",
            },
            "defaults": {
                "pollInterval": 45,
                "requestTimeout": 12,
                "lakematesPushUrl": "https://stage.lakemates.com/api/victron/ingest",
            },
            "integrations": [
                {
                    "boatSlug": "tranquility",
                    "boatName": "Tranquility",
                    "enabled": True,
                    "vrmToken": "token-a",
                    "vrmPortalId": "portal-a",
                    "lakemates": {
                        "siteKey": "stage-tranquility",
                        "ingestSecret": "push-secret",
                        "includeSummary": True,
                    },
                },
                {
                    "boatSlug": "apollo",
                    "enabled": False,
                    "vrmToken": "token-b",
                    "vrmPortalId": "portal-b",
                    "lakematesSiteKey": "stage-apollo",
                },
            ],
        }

        with patch.dict(os.environ, {"BRIDGE_CONFIG_JSON": json.dumps(config_payload)}, clear=False):
            config = bridge.load_bridge_config()

        assert config.database.host == "db"
        assert config.mqtt.prefix == "boats"
        assert len(config.integrations) == 2
        assert config.integrations[0].lakemates.include_summary is True
        assert config.integrations[1].enabled is False
        assert config.integrations[1].poll_interval == 45

    def test_legacy_config_remains_supported(self):
        env = {
            "BRIDGE_CONFIG_JSON": "",
            "BRIDGE_CONFIG_FILE": "",
            "DATABASE_HOST": "legacy-db",
            "DATABASE_NAME": "legacy-name",
            "DATABASE_USER": "legacy-user",
            "DATABASE_PASS": "legacy-pass",
            "VRM_TOKEN": "legacy-token",
            "VRM_PORTAL_ID": "legacy-portal",
            "LAKEMATES_PUSH_URL": "https://stage.example.com/ingest",
            "LAKEMATES_SITE_KEY": "legacy-site",
            "LAKEMATES_INGEST_SECRET": "legacy-secret",
            "BOAT_SLUG": "legacy-boat",
            "BOAT_NAME": "Legacy Boat",
            "VICTRON_CONFIG_PULL_ENABLED": "",
            "VICTRON_INTERNAL_API_BASE_URL": "",
            "VICTRON_BRIDGE_MACHINE_TOKEN": "",
            "VICTRON_ENCRYPTION_KEY": "",
        }

        with patch.dict(os.environ, env, clear=False):
            config = bridge.load_bridge_config()

        assert len(config.integrations) == 1
        assert config.integrations[0].boat_slug == "legacy-boat"
        assert config.integrations[0].lakemates.site_key == "legacy-site"

    def test_machine_config_can_load_without_static_integrations(self):
        config_payload = {
            "database": {
                "host": "db",
                "name": "victron",
                "user": "bridge",
                "password": "secret",
            },
            "lakematesInternal": {
                "enabled": True,
                "baseUrl": "https://stage.lakemates.com",
                "machineToken": "machine-token",
                "encryptionKey": base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii"),
            },
        }

        with patch.dict(os.environ, {"BRIDGE_CONFIG_JSON": json.dumps(config_payload)}, clear=False):
            config = bridge.load_bridge_config()

        assert config.lakemates_internal.enabled is True
        assert config.integrations == ()


class TestLakematesDecrypt:
    def test_decrypt_json_payload_uses_aes_gcm_contract(self):
        class FakeAESGCM:
            last_args = None

            def __init__(self, key):
                self.key = key

            def decrypt(self, nonce, ciphertext, aad):
                FakeAESGCM.last_args = (self.key, nonce, ciphertext, aad)
                return b'{"vrmToken":"token-a"}'

        payload = {
            "algorithm": "aes-256-gcm",
            "iv": base64.b64encode(b"123456789012").decode("ascii"),
            "ciphertext": base64.b64encode(b"cipher").decode("ascii"),
            "tag": base64.b64encode(b"tagtagtagtagtag1").decode("ascii"),
            "aad": base64.b64encode(b"boat:tranquility").decode("ascii"),
        }
        key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")

        with patch.object(bridge, "AESGCM", FakeAESGCM):
            result = bridge.decrypt_json_payload(payload, key)

        assert result["vrmToken"] == "token-a"
        assert FakeAESGCM.last_args == (
            b"0123456789abcdef0123456789abcdef",
            b"123456789012",
            b"ciphertagtagtagtagtag1",
            b"boat:tranquility",
        )

    def test_load_integration_from_lakemates_prefers_decrypted_credentials(self):
        machine = bridge.LakematesInternalConfig(
            enabled=True,
            base_url="https://stage.lakemates.com",
            machine_token="machine-token",
            encryption_key="unused-in-test",
        )
        item = {
            "boatSlug": "tranquility",
            "boatName": "Tranquility",
            "enabled": True,
            "vrmPortalId": "portal-a",
            "lakemates": {"siteKey": "site-a"},
            "encryptedCredentials": {"ciphertext": "x", "iv": "y", "tag": "z"},
        }

        with patch.object(bridge, "decrypt_json_payload", return_value={"vrmToken": "token-a"}):
            integration = bridge.load_integration_from_lakemates(item, {}, machine)

        assert integration.source == "lakemates-api"
        assert integration.vrm_token == "token-a"
        assert integration.vrm_portal_id == "portal-a"


class TestLakematesInternalApiClient:
    def test_fetch_integrations_builds_runtime_configs(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "defaults": {"pollInterval": 45},
            "integrations": [
                {
                    "boatSlug": "tranquility",
                    "boatName": "Tranquility",
                    "enabled": True,
                    "vrmPortalId": "portal-a",
                    "lakemates": {"siteKey": "site-a"},
                    "encryptedCredentials": {"ciphertext": "x", "iv": "y", "tag": "z"},
                }
            ],
        }
        session = Mock()
        session.headers = {}
        session.get.return_value = response
        client = bridge.LakematesInternalApiClient(
            bridge.LakematesInternalConfig(
                enabled=True,
                base_url="https://stage.lakemates.com",
                machine_token="machine-token",
                encryption_key="secret",
            ),
            session=session,
        )

        with patch.object(bridge, "decrypt_json_payload", return_value={"vrmToken": "token-a"}):
            integrations = client.fetch_integrations()

        assert len(integrations) == 1
        assert integrations[0].poll_interval == 45
        assert integrations[0].vrm_token == "token-a"
        assert session.get.call_args.args[0] == "https://stage.lakemates.com/api/internal/victron/integrations"
        assert session.headers["Authorization"] == "Bearer machine-token"

    def test_post_status_sends_sanitized_runtime_payload(self):
        response = Mock()
        response.raise_for_status.return_value = None
        session = Mock()
        session.headers = {}
        session.post.return_value = response
        client = bridge.LakematesInternalApiClient(
            bridge.LakematesInternalConfig(
                enabled=True,
                base_url="https://stage.lakemates.com",
                machine_token="machine-token",
                encryption_key="secret",
            ),
            session=session,
        )
        status = bridge.IntegrationStatus(
            boat_slug="tranquility",
            boat_name="Tranquility",
            site_key="site-a",
            enabled=True,
            runtime_status="healthy",
            last_error_message="VRM API request failed: HTTP 401 Unauthorized",
        )

        client.post_status(status)

        payload = json.loads(session.post.call_args.kwargs["data"])
        assert payload["boatKey"] == "tranquility"
        assert "boatSlug" not in payload
        assert "vrmToken" not in payload
        assert session.post.call_args.args[0] == "https://stage.lakemates.com/api/internal/victron/integrations/status"


class TestBridgeState:
    def test_store_metrics_updates_boat_buffer(self):
        state = bridge.BridgeState((self._integration("tranquility"),))
        state.store_metrics(
            "tranquility",
            {
                "battery_soc": {
                    "value": "85.3",
                    "value_num": 85.3,
                    "source_ts": "2026-01-01T00:00:00Z",
                    "group": "battery",
                    "unit": "percent",
                }
            },
        )

        snapshot = state.pop_metric_snapshot()
        assert snapshot["tranquility"]["battery_soc"]["value_num"] == 85.3

    def test_record_failure_isolated_per_boat(self):
        integrations = (self._integration("tranquility"), self._integration("apollo"))
        state = bridge.BridgeState(integrations)

        state.record_failure(integrations[0], "2026-04-11T16:00:00Z", "401", "bad token")

        statuses = {status.boat_slug: status for status in state.status_snapshot()}
        assert statuses["tranquility"].runtime_status == "error"
        assert statuses["tranquility"].consecutive_failures == 1
        assert statuses["apollo"].runtime_status == "starting"

    @staticmethod
    def _integration(boat_slug: str) -> bridge.IntegrationConfig:
        return bridge.IntegrationConfig(
            boat_slug=boat_slug,
            boat_name=boat_slug.title(),
            enabled=True,
            vrm_token="token",
            vrm_portal_id=f"{boat_slug}-portal",
            vrm_api_base="https://vrm.example.com/v2",
            poll_interval=30,
            request_timeout=10,
            lakemates=bridge.LakematesPushConfig(
                push_url="https://stage.example.com/ingest",
                site_key=f"{boat_slug}-site",
                ingest_secret="secret",
                timeout=5,
                include_summary=False,
            ),
        )


class TestWorkerManager:
    def test_reconcile_restarts_changed_worker_and_disables_removed_boat(self):
        state = bridge.BridgeState(())
        registry = bridge.IntegrationRegistry(())
        started = []

        def fake_thread(target, args, name, daemon):
            del target, daemon
            started.append((name, args[0].boat_slug))
            thread = Mock()
            thread.start = Mock()
            thread.join = Mock()
            return thread

        with patch.object(bridge.threading, "Thread", side_effect=fake_thread):
            manager = bridge.WorkerManager(state, registry, bridge.MqttConfig())
            one = self._integration("tranquility", poll_interval=30)
            changed = self._integration("tranquility", poll_interval=60)
            removed = self._integration("apollo", poll_interval=30)

            manager.reconcile((one, removed))
            manager.reconcile((changed,))

        assert started[0] == ("vrm-tranquility", "tranquility")
        assert started[1] == ("vrm-apollo", "apollo")
        assert started[2] == ("vrm-tranquility", "tranquility")
        statuses = {status.boat_slug: status for status in state.status_snapshot()}
        assert statuses["apollo"].runtime_status == "disabled"
        assert registry.get("tranquility").poll_interval == 60

    def test_reconcile_keeps_other_worker_when_one_config_changes(self):
        state = bridge.BridgeState(())
        registry = bridge.IntegrationRegistry(())
        started = []

        def fake_thread(target, args, name, daemon):
            del target, daemon
            started.append((name, args[0].boat_slug, args[0].poll_interval))
            thread = Mock()
            thread.start = Mock()
            thread.join = Mock()
            return thread

        with patch.object(bridge.threading, "Thread", side_effect=fake_thread):
            manager = bridge.WorkerManager(state, registry, bridge.MqttConfig())
            tranquility = self._integration("tranquility", poll_interval=30)
            apollo = self._integration("apollo", poll_interval=30)
            apollo_changed = self._integration("apollo", poll_interval=60)

            manager.reconcile((tranquility, apollo))
            manager.reconcile((tranquility, apollo_changed))

        assert started == [
            ("vrm-tranquility", "tranquility", 30),
            ("vrm-apollo", "apollo", 30),
            ("vrm-apollo", "apollo", 60),
        ]
        assert registry.get("tranquility").poll_interval == 30
        assert registry.get("apollo").poll_interval == 60

    @staticmethod
    def _integration(boat_slug: str, poll_interval: int) -> bridge.IntegrationConfig:
        return bridge.IntegrationConfig(
            boat_slug=boat_slug,
            boat_name=boat_slug.title(),
            enabled=True,
            vrm_token="token",
            vrm_portal_id=f"{boat_slug}-portal",
            vrm_api_base="https://vrm.example.com/v2",
            poll_interval=poll_interval,
            request_timeout=10,
            lakemates=bridge.LakematesPushConfig(site_key=f"{boat_slug}-site"),
        )


class TestLakematesPush:
    def test_noop_when_not_configured(self):
        integration = bridge.IntegrationConfig(
            boat_slug="tranquility",
            boat_name="Tranquility",
            enabled=True,
            vrm_token="token",
            vrm_portal_id="portal",
            vrm_api_base="https://vrm.example.com/v2",
            poll_interval=30,
            request_timeout=10,
            lakemates=bridge.LakematesPushConfig(),
        )

        assert bridge.push_lakemates(
            integration,
            {"battery_soc": {"value": "85", "value_num": 85.0, "source_ts": "2026-04-11T16:00:00Z"}},
            "2026-04-11T16:00:05Z",
        ) is False

    def test_posts_expected_payload_with_optional_summary(self):
        metrics = {
            "battery_soc": {"value": "85", "value_num": 85.0, "source_ts": "2026-04-11T16:00:00Z"},
            "solar_power": {"value": "420", "value_num": 420.0, "source_ts": "2026-04-11T16:00:01Z"},
        }
        integration = bridge.IntegrationConfig(
            boat_slug="tranquility",
            boat_name="Tranquility",
            enabled=True,
            vrm_token="token",
            vrm_portal_id="portal",
            vrm_api_base="https://vrm.example.com/v2",
            poll_interval=30,
            request_timeout=10,
            lakemates=bridge.LakematesPushConfig(
                push_url="https://stage.lakemates.com/api/victron/ingest",
                site_key="stage-victron",
                ingest_secret="push-secret",
                timeout=8,
                include_summary=True,
            ),
        )
        response = Mock()
        response.raise_for_status.return_value = None

        with patch.object(bridge.requests, "post", return_value=response) as mock_post:
            assert bridge.push_lakemates(integration, metrics, "2026-04-11T16:00:05Z") is True

        args, kwargs = mock_post.call_args
        assert args[0] == "https://stage.lakemates.com/api/victron/ingest"
        assert kwargs["headers"] == {
            "Content-Type": "application/json",
            "X-Ingest-Secret": "push-secret",
        }
        payload = json.loads(kwargs["data"])
        assert payload["siteKey"] == "stage-victron"
        assert payload["capturedAt"] == "2026-04-11T16:00:05Z"
        assert payload["summary"]["batterySoc"] == 85.0
        assert len(payload["metrics"]) == 2
        assert payload["metrics"][0]["metricKey"] == "battery_soc"


class TestSchemaSQL:
    def test_contains_required_tables(self):
        assert "victron_latest" in bridge.SCHEMA_SQL
        assert "victron_history" in bridge.SCHEMA_SQL
        assert "victron_bridge_status" in bridge.SCHEMA_SQL

    def test_contains_indexes(self):
        assert "idx_victron_history_boat_metric_time" in bridge.SCHEMA_SQL
        assert "idx_victron_status_runtime" in bridge.SCHEMA_SQL

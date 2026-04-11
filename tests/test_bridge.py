"""Unit tests for vrm bridge core logic."""

import os
import sys

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
        }
        assert mapped["solar_power"] == {
            "value": "512",
            "value_num": 512.0,
            "source_ts": "2026-04-11T16:05:00Z",
        }
        assert mapped["system_state"] == {
            "value": "Bulk",
            "value_num": None,
            "source_ts": "2026-04-11T16:05:00Z",
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


class TestStoreMetrics:
    def test_store_metrics_updates_buffer(self):
        bridge.metric_buffer.clear()
        bridge.store_metrics(
            {
                "battery_soc": {
                    "value": "85.3",
                    "value_num": 85.3,
                    "source_ts": "2026-01-01T00:00:00Z",
                }
            }
        )
        assert bridge.metric_buffer["battery_soc"]["value_num"] == 85.3
        bridge.metric_buffer.clear()


class TestSchemaSQL:
    def test_contains_required_tables(self):
        assert "victron_latest" in bridge.SCHEMA_SQL
        assert "victron_history" in bridge.SCHEMA_SQL

    def test_contains_indexes(self):
        assert "idx_victron_history_metric_time" in bridge.SCHEMA_SQL
        assert "idx_victron_history_time" in bridge.SCHEMA_SQL

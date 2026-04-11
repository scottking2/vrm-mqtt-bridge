-- Victron telemetry schema for CNPG Postgres
-- Tables are auto-created by the bridge on startup.
-- This file is for reference / manual bootstrap only.

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

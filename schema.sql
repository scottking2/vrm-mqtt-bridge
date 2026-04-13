-- Victron telemetry schema for CNPG Postgres
-- Tables are auto-created by the bridge on startup.
-- This file is for reference / manual bootstrap only.

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

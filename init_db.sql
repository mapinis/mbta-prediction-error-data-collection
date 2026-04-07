-- MBTA Arrival Prediction Collector
-- Database initialization script

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─────────────────────────────────────────────
-- trips
-- Lazy lookup cache populated on first sight of
-- a new trip_id. Queried once from /trips/{id}
-- and never re-fetched. Provides destination and
-- origin context not present in prediction events.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trips (
    trip_id       TEXT      PRIMARY KEY,
    route_id      TEXT      NOT NULL,
    headsign      TEXT      NOT NULL,                  -- destination, e.g. "Heath Street"
    direction_id  INTEGER   NOT NULL,
    fetched_at    TEXT      NOT NULL                    -- when this row was populated
);

-- ─────────────────────────────────────────────
-- prediction_snapshots
-- Append-only log of every add/update/remove
-- event received from the MBTA predictions stream.
-- Each row is one moment-in-time snapshot of a
-- prediction, used as a labeled training example
-- once the corresponding arrival is resolved.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prediction_snapshots (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id           TEXT    NOT NULL,
    stop_id                 TEXT    NOT NULL,
    stop_sequence           INTEGER NOT NULL,
    predicted_time          TEXT    NOT NULL,       -- ISO 8601 timestamp
    schedule_relationship   TEXT,                   -- null (SCHEDULED), ADDED, CANCELLED, SKIPPED, NO_DATA
    recorded_at             TEXT    NOT NULL,        -- ISO 8601 with milliseconds, e.g. 2026-03-29T14:32:01.123
    active_alert_count      INTEGER,                 -- Number of active alerts at this time
    max_alert_severity      INTEGER                 -- highest severity (0-10) among active alerts, null if none
);

CREATE INDEX IF NOT EXISTS idx_snapshots_prediction_recorded
    ON prediction_snapshots (prediction_id, recorded_at);
    
-- ─────────────────────────────────────────────
-- prediction_trips
-- One row per prediction, mapping it to its trip.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prediction_trips (
    prediction_id   TEXT    PRIMARY KEY,
    trip_id         TEXT    NOT NULL
);

-- ─────────────────────────────────────────────
-- arrivals
-- One row per cleanly resolved prediction.
-- Populated when a 'remove' event is received.
-- This is the ground truth target for modeling.
-- Information pulled from latest prediction snapshot
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS arrivals (
    prediction_id    TEXT    PRIMARY KEY,
    trip_id          TEXT    NOT NULL,
    stop_id          TEXT    NOT NULL,
    resolved_at      TEXT    NOT NULL,              -- when the remove event was received
    resolution_type  TEXT    NOT NULL               -- 'arrived', 'cancelled', 'skipped', 'other'
);

-- CREATE INDEX IF NOT EXISTS idx_arrivals_resolved_at
--    ON arrivals (resolved_at);

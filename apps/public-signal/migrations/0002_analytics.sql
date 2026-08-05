CREATE TABLE IF NOT EXISTS analytics_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_hash TEXT NOT NULL,
    event_type TEXT NOT NULL,
    target_slug TEXT NOT NULL,
    occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_analytics_events_time
    ON analytics_events(occurred_at);

CREATE INDEX IF NOT EXISTS idx_analytics_events_session_time
    ON analytics_events(session_hash, occurred_at);

CREATE INDEX IF NOT EXISTS idx_analytics_events_action_time
    ON analytics_events(event_type, target_slug, occurred_at);

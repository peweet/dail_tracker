CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    confirm_token TEXT NOT NULL UNIQUE,
    unsubscribe_token TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'active', 'unsubscribed')),
    cadence TEXT NOT NULL DEFAULT 'weekday' CHECK (cadence IN ('weekday', 'weekly')),
    sectors_json TEXT NOT NULL DEFAULT '[]',
    buyers_json TEXT NOT NULL DEFAULT '[]',
    min_value_eur INTEGER NOT NULL DEFAULT 0,
    deadline_days INTEGER NOT NULL DEFAULT 60,
    min_evidence INTEGER NOT NULL DEFAULT 70,
    include_expiries INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS delivery_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL,
    digest_key TEXT NOT NULL,
    provider_message_id TEXT,
    opportunity_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_once
    ON delivery_log(subscription_id, digest_key);

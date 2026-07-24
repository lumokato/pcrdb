\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE IF NOT EXISTS arena_alert_state (
    viewer_id BIGINT PRIMARY KEY,
    user_name TEXT NOT NULL DEFAULT '',
    arena_rank INTEGER NOT NULL CHECK (arena_rank >= 0),
    grand_arena_rank INTEGER NOT NULL CHECK (grand_arena_rank >= 0),
    last_checked_at TIMESTAMPTZ NOT NULL,
    last_notified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;

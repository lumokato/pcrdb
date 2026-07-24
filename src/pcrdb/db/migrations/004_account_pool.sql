\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS pool_enabled BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE accounts
SET pool_enabled = FALSE,
    updated_at = NOW()
WHERE note = 'Dedicated account migrated from stopped pcrjjc-tg for arena alerts'
  AND pool_enabled = TRUE;

CREATE TABLE IF NOT EXISTS account_pool_state (
    account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    last_acquired_at TIMESTAMPTZ,
    last_released_at TIMESTAMPTZ,
    last_purpose TEXT,
    acquire_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    cooldown_until TIMESTAMPTZ,
    last_error_type TEXT
);

INSERT INTO account_pool_state (account_id)
SELECT id
FROM accounts
ON CONFLICT (account_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_account_pool_available
    ON account_pool_state (cooldown_until, last_acquired_at, account_id);

COMMIT;

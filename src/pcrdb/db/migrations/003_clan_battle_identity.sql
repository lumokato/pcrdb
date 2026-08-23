\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE clan_battle.periods
    ADD COLUMN IF NOT EXISTS clan_battle_id BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_clan_battle_period_battle_id
    ON clan_battle.periods(clan_battle_id)
    WHERE clan_battle_id IS NOT NULL;

ALTER TABLE clan_battle.worker_state
    ADD COLUMN IF NOT EXISTS active_clan_battle_id BIGINT;

COMMIT;

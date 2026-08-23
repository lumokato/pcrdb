\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE clan_battle.import_files
    ADD COLUMN IF NOT EXISTS corrections JSONB NOT NULL DEFAULT '[]'::jsonb;

DO $$
DECLARE
    primary_key_name TEXT;
BEGIN
    -- The database entrypoint replays every migration on each container start.
    -- Once 004_allow_tied_ranks.sql has created its compatibility sequence,
    -- this older migration must not restore the obsolete unique-rank key.
    IF to_regclass('clan_battle.rankings_row_number_seq') IS NOT NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = 'clan_battle.rankings'::regclass
          AND attname = 'row_number'
          AND NOT attisdropped
    ) THEN
        IF EXISTS (
            SELECT 1
            FROM clan_battle.rankings
            GROUP BY snapshot_id, rank
            HAVING COUNT(*) > 1
        ) THEN
            RAISE EXCEPTION
                'cannot enforce unique clan battle ranks while duplicate ranks are stored';
        END IF;

        SELECT conname
        INTO primary_key_name
        FROM pg_constraint
        WHERE conrelid = 'clan_battle.rankings'::regclass
          AND contype = 'p';

        IF primary_key_name IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE clan_battle.rankings DROP CONSTRAINT %I',
                primary_key_name
            );
        END IF;

        ALTER TABLE clan_battle.rankings DROP COLUMN row_number;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'clan_battle.rankings'::regclass
          AND contype = 'p'
    ) THEN
        ALTER TABLE clan_battle.rankings
            ADD PRIMARY KEY (snapshot_id, rank);
    END IF;
END
$$;

DROP INDEX IF EXISTS clan_battle.idx_clan_battle_rankings_rank;

COMMIT;

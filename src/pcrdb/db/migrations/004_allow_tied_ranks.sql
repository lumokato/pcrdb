\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE clan_battle.rankings
    ADD COLUMN IF NOT EXISTS row_number INTEGER;

DO $$
BEGIN
    IF NOT (
        SELECT attnotnull
        FROM pg_attribute
        WHERE attrelid = 'clan_battle.rankings'::regclass
          AND attname = 'row_number'
          AND NOT attisdropped
    ) THEN
        WITH numbered AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY snapshot_id
                       ORDER BY rank, clan_name, leader_name, damage DESC, ctid
                   )::INTEGER AS value
            FROM clan_battle.rankings
        )
        UPDATE clan_battle.rankings target
        SET row_number = numbered.value
        FROM numbered
        WHERE target.ctid = numbered.ctid
          AND target.row_number IS NULL;

        ALTER TABLE clan_battle.rankings
            ALTER COLUMN row_number SET NOT NULL;
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('clan_battle.rankings_row_number_seq') IS NULL THEN
        CREATE SEQUENCE clan_battle.rankings_row_number_seq;
        PERFORM setval(
            'clan_battle.rankings_row_number_seq',
            COALESCE((SELECT MAX(row_number) + 1 FROM clan_battle.rankings), 1),
            FALSE
        );
    END IF;
END
$$;

ALTER TABLE clan_battle.rankings
    ALTER COLUMN row_number
    SET DEFAULT nextval('clan_battle.rankings_row_number_seq');

DO $$
DECLARE
    primary_key_name TEXT;
    primary_key_columns TEXT[];
BEGIN
    SELECT constraint_value.conname,
           ARRAY_AGG(attribute_value.attname ORDER BY key_value.ordinality)
    INTO primary_key_name, primary_key_columns
    FROM pg_constraint constraint_value
    CROSS JOIN LATERAL UNNEST(constraint_value.conkey)
        WITH ORDINALITY AS key_value(attnum, ordinality)
    JOIN pg_attribute attribute_value
      ON attribute_value.attrelid = constraint_value.conrelid
     AND attribute_value.attnum = key_value.attnum
    WHERE constraint_value.conrelid = 'clan_battle.rankings'::regclass
      AND constraint_value.contype = 'p'
    GROUP BY constraint_value.conname;

    IF primary_key_columns IS DISTINCT FROM ARRAY['snapshot_id', 'row_number']::TEXT[] THEN
        IF primary_key_name IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE clan_battle.rankings DROP CONSTRAINT %I',
                primary_key_name
            );
        END IF;

        ALTER TABLE clan_battle.rankings
            ADD PRIMARY KEY (snapshot_id, row_number);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_clan_battle_rankings_snapshot_rank
    ON clan_battle.rankings(snapshot_id, rank, row_number);

COMMIT;

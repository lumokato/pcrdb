\set ON_ERROR_STOP on

BEGIN;

DO $$
DECLARE
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO duplicate_count
    FROM (
        SELECT snapshot_id, rank
        FROM clan_battle.rankings
        GROUP BY snapshot_id, rank
        HAVING COUNT(*) > 1
    ) duplicates;

    IF duplicate_count = 0 THEN
        CREATE UNIQUE INDEX IF NOT EXISTS uq_clan_battle_rankings_snapshot_rank
            ON clan_battle.rankings(snapshot_id, rank);
    ELSE
        RAISE NOTICE
            'unique clan battle rank index deferred while % duplicate snapshot ranks remain',
            duplicate_count;
    END IF;
END
$$;

COMMIT;

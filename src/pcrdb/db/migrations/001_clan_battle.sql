\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS clan_battle;

CREATE TABLE IF NOT EXISTS clan_battle.periods (
    period DATE PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'final'
        CHECK (status IN ('waiting_start', 'active', 'settlement', 'final')),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ,
    final_snapshot_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (EXTRACT(DAY FROM period) = 1)
);

CREATE TABLE IF NOT EXISTS clan_battle.snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    period DATE NOT NULL REFERENCES clan_battle.periods(period) ON DELETE RESTRICT,
    captured_at TIMESTAMPTZ,
    snapshot_type TEXT NOT NULL
        CHECK (snapshot_type IN ('progress', 'final_candidate', 'monthly_final')),
    source TEXT NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    min_rank INTEGER,
    max_rank INTEGER,
    content_sha256 CHAR(64) NOT NULL,
    probe_sha256 CHAR(64) NOT NULL,
    is_final BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE NULLS NOT DISTINCT (period, captured_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_clan_battle_final_snapshot
    ON clan_battle.snapshots(period)
    WHERE is_final;

CREATE INDEX IF NOT EXISTS idx_clan_battle_snapshots_period_time
    ON clan_battle.snapshots(period DESC, captured_at DESC NULLS LAST);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'periods_final_snapshot_fk'
          AND conrelid = 'clan_battle.periods'::regclass
    ) THEN
        ALTER TABLE clan_battle.periods
            ADD CONSTRAINT periods_final_snapshot_fk
            FOREIGN KEY (final_snapshot_id)
            REFERENCES clan_battle.snapshots(snapshot_id)
            ON DELETE SET NULL;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS clan_battle.rankings (
    snapshot_id BIGINT NOT NULL
        REFERENCES clan_battle.snapshots(snapshot_id) ON DELETE CASCADE,
    rank INTEGER NOT NULL CHECK (rank > 0),
    clan_name TEXT NOT NULL,
    leader_name TEXT NOT NULL,
    member_num SMALLINT NOT NULL DEFAULT 0,
    damage BIGINT NOT NULL DEFAULT 0,
    lap INTEGER NOT NULL DEFAULT 1,
    boss_id SMALLINT NOT NULL DEFAULT 1,
    remain BIGINT NOT NULL DEFAULT 0,
    grade_rank INTEGER NOT NULL DEFAULT 0,
    bili_rank INTEGER,
    PRIMARY KEY (snapshot_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_clan_battle_rankings_clan_name
    ON clan_battle.rankings USING gin (to_tsvector('simple', clan_name));

CREATE TABLE IF NOT EXISTS clan_battle.import_files (
    import_file_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    source_size BIGINT NOT NULL,
    source_mtime TIMESTAMPTZ,
    encoding TEXT NOT NULL,
    header JSONB NOT NULL,
    file_sha256 CHAR(64) NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    row_count INTEGER NOT NULL,
    inferred_period DATE,
    inferred_captured_at TIMESTAMPTZ,
    snapshot_id BIGINT REFERENCES clan_battle.snapshots(snapshot_id) ON DELETE RESTRICT,
    status TEXT NOT NULL
        CHECK (status IN ('imported', 'duplicate', 'prefix_duplicate', 'invalid', 'conflict')),
    duplicate_of BIGINT REFERENCES clan_battle.import_files(import_file_id) ON DELETE SET NULL,
    corrections JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_clan_battle_import_hash
    ON clan_battle.import_files(file_sha256);

CREATE TABLE IF NOT EXISTS clan_battle.collection_runs (
    run_id BIGSERIAL PRIMARY KEY,
    trigger_name TEXT NOT NULL,
    phase_before TEXT NOT NULL,
    phase_after TEXT NOT NULL,
    result_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    pages_fetched INTEGER NOT NULL DEFAULT 0,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    snapshot_id BIGINT REFERENCES clan_battle.snapshots(snapshot_id) ON DELETE SET NULL,
    error_message TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_clan_battle_runs_started
    ON clan_battle.collection_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS clan_battle.worker_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    phase TEXT NOT NULL DEFAULT 'final'
        CHECK (phase IN ('waiting_start', 'active', 'settlement', 'final')),
    active_period DATE,
    reference_probe_sha256 CHAR(64),
    candidate_content_sha256 CHAR(64),
    candidate_seen_count INTEGER NOT NULL DEFAULT 0,
    successful_empty_count INTEGER NOT NULL DEFAULT 0,
    last_probe_at TIMESTAMPTZ,
    last_nonempty_at TIMESTAMPTZ,
    last_snapshot_id BIGINT REFERENCES clan_battle.snapshots(snapshot_id) ON DELETE SET NULL,
    last_error_type TEXT,
    last_error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO clan_battle.worker_state(singleton)
VALUES (TRUE)
ON CONFLICT (singleton) DO NOTHING;

COMMIT;

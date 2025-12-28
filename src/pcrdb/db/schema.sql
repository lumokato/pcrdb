-- pcrdb PostgreSQL Schema
-- Version: 2.1
-- Date: 2025-12-28

-----------------------------------------------------------
-- Table 1: clan_snapshots
-----------------------------------------------------------
CREATE TABLE clan_snapshots (
    id SERIAL PRIMARY KEY,
    clan_id INTEGER NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    
    clan_name TEXT,
    leader_viewer_id BIGINT,
    leader_name TEXT,
    join_condition SMALLINT,
    activity SMALLINT,
    clan_battle_mode SMALLINT,
    member_num SMALLINT,
    current_period_ranking INTEGER,
    grade_rank INTEGER,
    description TEXT,
    exist BOOLEAN DEFAULT TRUE,
    
    UNIQUE (clan_id, collected_at)
);

CREATE INDEX idx_clan_latest ON clan_snapshots(clan_id, collected_at DESC);

-----------------------------------------------------------
-- Table 2: player_clan_snapshots
-----------------------------------------------------------
CREATE TABLE player_clan_snapshots (
    id SERIAL PRIMARY KEY,
    viewer_id BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    
    name TEXT,
    level SMALLINT,
    role SMALLINT,
    total_power INTEGER,
    join_clan_id INTEGER,
    join_clan_name TEXT,
    last_login_time TIMESTAMPTZ,
    
    UNIQUE (viewer_id, collected_at)
);

CREATE INDEX idx_pclan_viewer ON player_clan_snapshots(viewer_id, collected_at DESC);
CREATE INDEX idx_pclan_clan ON player_clan_snapshots(join_clan_id);

-----------------------------------------------------------
-- Table 3: player_profile_snapshots
-----------------------------------------------------------
CREATE TABLE player_profile_snapshots (
    id SERIAL PRIMARY KEY,
    viewer_id BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    
    user_name TEXT,
    team_level SMALLINT,
    unit_num SMALLINT,
    total_power INTEGER,
    join_clan_id INTEGER,
    join_clan_name TEXT,
    arena_rank SMALLINT,
    arena_group SMALLINT,
    grand_arena_rank SMALLINT,
    grand_arena_group SMALLINT,
    favorite_unit INTEGER,
    princess_knight_rank_total_exp BIGINT,
    talent_quest_clear JSONB,
    user_comment TEXT,
    last_login_time TIMESTAMPTZ,
    
    UNIQUE (viewer_id, collected_at)
);

CREATE INDEX idx_pprofile_viewer ON player_profile_snapshots(viewer_id, collected_at DESC);

-----------------------------------------------------------
-- Table 4: grand_arena_snapshots
-----------------------------------------------------------
CREATE TABLE grand_arena_snapshots (
    id SERIAL PRIMARY KEY,
    viewer_id BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    
    user_name TEXT,
    team_level SMALLINT,
    grand_arena_rank SMALLINT,
    grand_arena_group SMALLINT,
    winning_number SMALLINT,
    favorite_unit INTEGER,
    
    UNIQUE (viewer_id, collected_at)
);

CREATE INDEX idx_grand_viewer ON grand_arena_snapshots(viewer_id, collected_at DESC);

-----------------------------------------------------------
-- Table 5: arena_deck_snapshots
-----------------------------------------------------------
CREATE TABLE arena_deck_snapshots (
    id SERIAL PRIMARY KEY,
    viewer_id BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    
    user_name TEXT,
    team_level SMALLINT,
    arena_group SMALLINT,
    arena_rank SMALLINT,
    arena_deck JSONB,
    
    UNIQUE (viewer_id, collected_at)
);

CREATE INDEX idx_deck_viewer ON arena_deck_snapshots(viewer_id, collected_at DESC);

-----------------------------------------------------------
-- Table 6: accounts (data collection accounts)
-----------------------------------------------------------
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    uid TEXT NOT NULL UNIQUE,             -- login UID (string format)
    access_key TEXT NOT NULL,             -- shared login key
    
    -- account info (obtained after login)
    viewer_id BIGINT,                     -- player ID
    name TEXT,                            -- player name
    
    -- arena info (obtained from ranking API)
    arena_group SMALLINT DEFAULT 0,       -- JJC group (0=not queried)
    grand_arena_group SMALLINT DEFAULT 0, -- PJJC group (0=not enabled or not queried)
    
    -- status
    is_active BOOLEAN DEFAULT TRUE,
    note TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

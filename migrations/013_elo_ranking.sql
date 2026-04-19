-- Hybrid ranking for Belgian Carcassonne players.
-- Base: normalized BGA all-time Elo.
-- Bonuses: competitive performance (BK live, BK online, WCC, nationals) with recency decay.
-- Populated by scripts/compute_ranking.py.

CREATE TABLE IF NOT EXISTS player_ranking (
    player_id         INTEGER PRIMARY KEY,
    bga_base          REAL,            -- 0-100 normalized BGA all-time ELO
    bga_peak_elo      REAL,            -- raw peak elo_after from game_players
    bga_current_elo   REAL,            -- latest elo_after
    bga_games         INTEGER,         -- total BGA games with elo_after set
    bk_live_bonus     REAL NOT NULL DEFAULT 0,
    bk_online_bonus   REAL NOT NULL DEFAULT 0,
    wcc_bonus         REAL NOT NULL DEFAULT 0,
    nations_bonus     REAL NOT NULL DEFAULT 0,
    total_score       REAL NOT NULL DEFAULT 0,
    ranking_elo       INTEGER NOT NULL DEFAULT 1500,   -- chess-style 1500..~2000+
    rank              INTEGER,
    computed_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_player_ranking_total ON player_ranking(total_score DESC);

-- Per-contribution breakdown (for transparency in the dashboard).
CREATE SEQUENCE IF NOT EXISTS player_ranking_events_id_seq;

CREATE TABLE IF NOT EXISTS player_ranking_events (
    id           INTEGER PRIMARY KEY DEFAULT nextval('player_ranking_events_id_seq'),
    player_id    INTEGER NOT NULL,
    source       VARCHAR NOT NULL,  -- bk_live | bk_online | wcc | nations_official | nations_friendly
    event_date   DATE,
    event_year   INTEGER,
    tournament_id INTEGER,
    description  VARCHAR,
    raw_points   REAL    NOT NULL,
    decay        REAL    NOT NULL,
    points       REAL    NOT NULL   -- raw_points * decay * source_weight
);

CREATE INDEX IF NOT EXISTS idx_prk_events_player ON player_ranking_events(player_id, source);

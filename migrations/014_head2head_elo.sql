-- Head-to-head Elo ranking for Belgian players.
-- Populated by scripts/compute_head2head_elo.py.
-- Counts only games where both participants are Belgian (country = 'BE').

CREATE TABLE IF NOT EXISTS player_head2head_elo (
    player_id     INTEGER PRIMARY KEY,
    rating        REAL    NOT NULL,
    peak_rating   REAL    NOT NULL,
    peak_date     DATE,
    games         INTEGER NOT NULL DEFAULT 0,
    wins          INTEGER NOT NULL DEFAULT 0,
    losses        INTEGER NOT NULL DEFAULT 0,
    draws         INTEGER NOT NULL DEFAULT 0,
    last_played   DATE,
    rank          INTEGER,
    computed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_h2h_rating ON player_head2head_elo(rating DESC);

CREATE SEQUENCE IF NOT EXISTS player_h2h_events_id_seq;

CREATE TABLE IF NOT EXISTS player_head2head_events (
    id             INTEGER PRIMARY KEY DEFAULT nextval('player_h2h_events_id_seq'),
    player_id      INTEGER NOT NULL,
    event_date     DATE,
    source         VARCHAR NOT NULL,  -- bga | bclc_swiss | bclc_playoff | bcoc
    tournament_id  INTEGER,
    opponent_id    INTEGER NOT NULL,
    result         REAL    NOT NULL,  -- 1 / 0.5 / 0
    k_factor       REAL    NOT NULL,
    rating_before  REAL    NOT NULL,
    rating_after   REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_h2h_events_player ON player_head2head_events(player_id, event_date);

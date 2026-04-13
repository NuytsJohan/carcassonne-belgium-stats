-- Migration 009: tournament_matches table for BCOC and similar 1v1 tournament data

CREATE SEQUENCE IF NOT EXISTS tournament_matches_id_seq;
CREATE TABLE IF NOT EXISTS tournament_matches (
    id              INTEGER PRIMARY KEY DEFAULT nextval('tournament_matches_id_seq'),
    tournament_id   INTEGER NOT NULL REFERENCES tournaments(id),
    stage           TEXT NOT NULL,        -- 'Group A', 'Round of 16', '1/4 Finals', '1/2 Finals', 'Final', 'Best 3rd'
    match_number    INTEGER,              -- ordering/label (e.g., 81-88 for R16)
    player_1_id     INTEGER NOT NULL REFERENCES players(id),
    player_2_id     INTEGER NOT NULL REFERENCES players(id),
    score_1         INTEGER,              -- games won by player 1
    score_2         INTEGER,              -- games won by player 2
    result          TEXT CHECK (result IN ('1', '2', 'D')),  -- winner: '1', '2', or 'D' draw
    notes           TEXT,                 -- 'wo' (walkover), etc.
    game_id_1       INTEGER REFERENCES games(id),
    game_id_2       INTEGER REFERENCES games(id),
    game_id_3       INTEGER REFERENCES games(id),
    game_id_4       INTEGER REFERENCES games(id),
    game_id_5       INTEGER REFERENCES games(id),
    UNIQUE (tournament_id, player_1_id, player_2_id, stage)
);
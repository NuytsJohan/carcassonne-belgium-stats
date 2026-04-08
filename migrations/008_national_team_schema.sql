-- Migration 008: national team competition schema
-- Requires full DB rebuild (DuckDB cannot ALTER CHECK constraints)
-- Backup: data/carcassonne.duckdb.bak

-- 1. tournaments: added types FRIENDLIES, ETCOC, WTCOC (removed NATIONS)
--    added column: national_team_competition BOOLEAN DEFAULT FALSE
--    CHECK (type IN ('BK', 'BCOC', 'BCL', 'FRIENDLIES', 'ETCOC', 'WTCOC', 'OTHER'))

-- 2. nations_competition_duels: Belgium vs Country X on a given day
--    One row per confrontation (e.g., Belgium vs Netherlands in Friendly 1)
CREATE TABLE nations_competition_duels (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('nations_competition_duels_id_seq'),
    tournament_id       INTEGER NOT NULL REFERENCES tournaments(id),
    opponent_country    TEXT NOT NULL,
    stage               TEXT,          -- e.g., 'Group A', 'Quarter-final', 'Friendly 3'
    date_played         DATE,
    UNIQUE (tournament_id, opponent_country, stage)
);

-- 3. nations_matches: individual player vs player match within a duel
--    One row per Belgian player vs opponent (e.g., N2xU vs zwollywood)
CREATE TABLE nations_matches (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('nations_matches_id_seq'),
    duel_id             INTEGER NOT NULL REFERENCES nations_competition_duels(id),
    player_id           INTEGER NOT NULL REFERENCES players(id),      -- Belgian player
    opponent_player_id  INTEGER NOT NULL REFERENCES players(id),      -- Opponent player
    score_belgium       INTEGER,       -- total score Belgian player
    score_opponent      INTEGER,       -- total score opponent
    result              TEXT NOT NULL CHECK (result IN ('W', 'L')),
    game_id_1           INTEGER REFERENCES games(id),  -- BGA game links (best of 3 or 5)
    game_id_2           INTEGER REFERENCES games(id),
    game_id_3           INTEGER REFERENCES games(id),
    game_id_4           INTEGER REFERENCES games(id),
    game_id_5           INTEGER REFERENCES games(id),
    notes               TEXT,          -- 'timeout', 'concede', 'noshow', 'lost draw', etc.
    UNIQUE (duel_id, player_id)
);

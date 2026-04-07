-- Carcassonne Belgium Stats — Initial Schema
-- COM-31

CREATE SEQUENCE IF NOT EXISTS players_id_seq;
CREATE TABLE IF NOT EXISTS players (
    id          INTEGER PRIMARY KEY DEFAULT nextval('players_id_seq'),
    name        TEXT NOT NULL,
    name_nl     TEXT,
    wica_id     TEXT,
    bga_player_id TEXT,
    country     TEXT,
    created_at  TIMESTAMP DEFAULT current_timestamp
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_bga ON players(bga_player_id) WHERE bga_player_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS player_aliases (
    alias       TEXT PRIMARY KEY,
    player_id   INTEGER NOT NULL REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS tournaments (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('BK', 'BCOC', 'BCL', 'NATIONS', 'OTHER')),
    year        INTEGER NOT NULL,
    edition     TEXT,
    location    TEXT,
    date_start  DATE,
    date_end    DATE,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS tournament_participants (
    id              INTEGER PRIMARY KEY,
    tournament_id   INTEGER NOT NULL REFERENCES tournaments(id),
    player_id       INTEGER NOT NULL REFERENCES players(id),
    final_rank      INTEGER,
    total_score     REAL,
    UNIQUE (tournament_id, player_id)
);

CREATE SEQUENCE IF NOT EXISTS games_id_seq;
CREATE TABLE IF NOT EXISTS games (
    id              INTEGER PRIMARY KEY DEFAULT nextval('games_id_seq'),
    tournament_id   INTEGER REFERENCES tournaments(id),  -- NULL voor BGA spellen
    round           INTEGER,                              -- NULL voor BGA spellen
    table_number    INTEGER,
    bga_table_id    TEXT,
    source          TEXT DEFAULT 'manual',
    played_at       TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_games_bga ON games(bga_table_id) WHERE bga_table_id IS NOT NULL;

CREATE SEQUENCE IF NOT EXISTS game_players_id_seq;
CREATE TABLE IF NOT EXISTS game_players (
    id          INTEGER PRIMARY KEY DEFAULT nextval('game_players_id_seq'),
    game_id     INTEGER NOT NULL REFERENCES games(id),
    player_id   INTEGER NOT NULL REFERENCES players(id),
    score       REAL,
    rank        INTEGER,
    elo_delta   INTEGER,
    elo_after   INTEGER,
    conceded    BOOLEAN DEFAULT FALSE,
    UNIQUE (game_id, player_id)
);

-- Nationale ploeg competities
CREATE TABLE IF NOT EXISTS nations_competitions (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    year        INTEGER NOT NULL,
    host        TEXT,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS nations_matches (
    id                  INTEGER PRIMARY KEY,
    competition_id      INTEGER NOT NULL REFERENCES nations_competitions(id),
    round               INTEGER,
    team_a              TEXT NOT NULL,
    team_b              TEXT NOT NULL,
    team_a_score        REAL,
    team_b_score        REAL,
    result              TEXT CHECK (result IN ('W', 'D', 'L', NULL))
);

CREATE TABLE IF NOT EXISTS nations_match_players (
    id          INTEGER PRIMARY KEY,
    match_id    INTEGER NOT NULL REFERENCES nations_matches(id),
    player_id   INTEGER NOT NULL REFERENCES players(id),
    team        TEXT NOT NULL,
    score       REAL,
    UNIQUE (match_id, player_id)
);

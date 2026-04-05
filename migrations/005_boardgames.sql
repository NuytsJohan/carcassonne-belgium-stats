-- Migration 005: bordspellen tabel, koppeling in games, import tracking

-- Bordspellen lookup tabel
CREATE SEQUENCE IF NOT EXISTS boardgames_id_seq;
CREATE TABLE IF NOT EXISTS boardgames (
    id      INTEGER PRIMARY KEY DEFAULT nextval('boardgames_id_seq'),
    name    TEXT NOT NULL,
    bga_id  INTEGER,            -- BGA game_id zoals gebruikt in de API
    notes   TEXT
);

-- Vaste IDs voor bekende spellen (idempotent via WHERE NOT EXISTS)
INSERT INTO boardgames (id, name, bga_id)
SELECT 1, 'Carcassonne', 1
WHERE NOT EXISTS (SELECT 1 FROM boardgames WHERE id = 1);

INSERT INTO boardgames (id, name, bga_id)
SELECT 2, 'Framework', 1889
WHERE NOT EXISTS (SELECT 1 FROM boardgames WHERE id = 2);

-- Koppel games aan een bordspel
-- Noot: DuckDB ondersteunt geen REFERENCES in ALTER TABLE ADD COLUMN
ALTER TABLE games ADD COLUMN IF NOT EXISTS boardgame_id INTEGER;

-- Zet bestaande BGA spellen op Carcassonne
UPDATE games SET boardgame_id = 1 WHERE boardgame_id IS NULL AND source = 'bga';

-- Import tracking: laatste succesvolle import per (bga_player_id, boardgame)
-- last_ended_at = ended_at van het meest recente geïmporteerde spel
-- Volgende keer halen we enkel spellen op met ended_at > last_ended_at
CREATE TABLE IF NOT EXISTS import_tracking (
    bga_player_id  TEXT    NOT NULL,
    boardgame_id   INTEGER NOT NULL REFERENCES boardgames(id),
    last_ended_at  TIMESTAMP,
    imported_at    TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (bga_player_id, boardgame_id)
);

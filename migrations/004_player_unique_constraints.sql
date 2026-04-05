-- Migration 004: unieke constraints op players
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_name ON players(name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_bga_id ON players(bga_player_id);

-- Migration 010: Add stats columns to tournament_participants

ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS duels_played INTEGER;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS duels_won INTEGER;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS duels_lost INTEGER;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS games_won INTEGER;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS games_lost INTEGER;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS win_pct FLOAT;
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS resistance FLOAT;
-- Migration 003: extra BGA velden voor games en game_players

ALTER TABLE games ADD COLUMN IF NOT EXISTS ended_at       TIMESTAMP;
ALTER TABLE games ADD COLUMN IF NOT EXISTS duration_min   INTEGER;   -- speelduur in minuten
ALTER TABLE games ADD COLUMN IF NOT EXISTS unranked        BOOLEAN DEFAULT FALSE;
ALTER TABLE games ADD COLUMN IF NOT EXISTS normal_end      BOOLEAN DEFAULT TRUE;
ALTER TABLE games ADD COLUMN IF NOT EXISTS ranking_disabled BOOLEAN DEFAULT FALSE;

ALTER TABLE game_players ADD COLUMN IF NOT EXISTS elo_penalty  INTEGER;
ALTER TABLE game_players ADD COLUMN IF NOT EXISTS arena_win    BOOLEAN;
ALTER TABLE game_players ADD COLUMN IF NOT EXISTS arena_after  INTEGER;

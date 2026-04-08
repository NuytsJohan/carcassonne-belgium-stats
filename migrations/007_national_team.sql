-- Migration 007: national team flag + alias jorenderidder → MarathonMeeple

-- 1. Add national_team flag to players
-- Note: DuckDB may error if column exists due to dependencies; safe to ignore
ALTER TABLE players ADD COLUMN national_team BOOLEAN DEFAULT FALSE;

-- 2. Flag all known Belgian national team members (from Team Belgium Stats.xlsx)
UPDATE players SET national_team = TRUE
WHERE name IN (
    '71Knives',
    'AnTHology',
    'Bangla',
    'Bids',
    'Carcharoth 9',
    'CraftyRaf',
    'Defdamesdompi',
    'FabianM_be',
    'JinaJina',
    'Learn to fly',
    'MarathonMeeple',
    'N2xU',
    'Nicedicer',
    'Sicarius Lupus',
    'arinius',
    'ludojahhjahh',
    'mobidic',
    'obiwonder',
    'rally8',
    'valmir79',
    'vanbaekel-'
);

-- 3. Add alias for old BGA username
INSERT INTO player_aliases (alias, player_id)
SELECT 'jorenderidder', id FROM players WHERE name = 'MarathonMeeple'
WHERE NOT EXISTS (SELECT 1 FROM player_aliases WHERE alias = 'jorenderidder');

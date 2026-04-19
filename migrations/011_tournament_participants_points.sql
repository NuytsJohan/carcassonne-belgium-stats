-- Adds match-points column ('punten' in BCLC PDFs) to tournament_participants.
ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS points REAL;

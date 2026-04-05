-- Migration 006: adres veld voor spelers

ALTER TABLE players ADD COLUMN IF NOT EXISTS address TEXT;

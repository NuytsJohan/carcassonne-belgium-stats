"""Migration 012: add WCC tournament type + participants_count + tp.note.

DuckDB cannot ALTER a CHECK constraint, so we EXPORT the DB, patch the
schema DDL, and IMPORT into a fresh file. Adds the two new columns after
import via ALTER TABLE.
"""
import shutil
from pathlib import Path

import duckdb

ROOT = Path(__file__).parents[1]
DB = ROOT / "data" / "carcassonne.duckdb"
BAK = ROOT / "data" / "carcassonne.duckdb.pre_wcc.bak"
EXPORT_DIR = ROOT / "data" / "_export_wcc"

OLD_CHECK = "CHECK((\"type\" IN ('BK', 'BCOC', 'BCL', 'BCLC', 'FRIENDLIES', 'ETCOC', 'WTCOC', 'OTHER')))"
NEW_CHECK = "CHECK((\"type\" IN ('BK', 'BCOC', 'BCL', 'BCLC', 'FRIENDLIES', 'ETCOC', 'WTCOC', 'WCC', 'OTHER')))"


def main():
    if BAK.exists():
        print(f"Backup already exists at {BAK}; aborting to avoid overwrite.")
        return
    print(f"Backing up {DB} -> {BAK}")
    shutil.copy(DB, BAK)

    if EXPORT_DIR.exists():
        shutil.rmtree(EXPORT_DIR)

    conn = duckdb.connect(str(DB))
    existing = conn.execute(
        "SELECT constraint_text FROM duckdb_constraints() "
        "WHERE table_name='tournaments' AND constraint_text LIKE '%WCC%'"
    ).fetchone()
    if existing:
        print("WCC already in CHECK; nothing to do.")
        conn.close()
        return

    print(f"Exporting DB to {EXPORT_DIR}")
    conn.execute(f"EXPORT DATABASE '{EXPORT_DIR.as_posix()}' (FORMAT PARQUET)")
    conn.close()

    schema_file = EXPORT_DIR / "schema.sql"
    schema = schema_file.read_text(encoding="utf-8")
    if OLD_CHECK not in schema:
        raise RuntimeError("Could not find tournaments CHECK clause to patch.")
    schema = schema.replace(OLD_CHECK, NEW_CHECK)
    schema_file.write_text(schema, encoding="utf-8")
    print("Patched schema.sql with WCC type.")

    DB.unlink()
    conn = duckdb.connect(str(DB))
    print("Re-importing into fresh DB...")
    conn.execute(f"IMPORT DATABASE '{EXPORT_DIR.as_posix()}'")

    print("Adding tournaments.participants_count and tournament_participants.note")
    conn.execute("ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS participants_count INTEGER")
    conn.execute("ALTER TABLE tournament_participants ADD COLUMN IF NOT EXISTS note TEXT")

    types = [r[0] for r in conn.execute("SELECT DISTINCT type FROM tournaments").fetchall()]
    print(f"Tournament types after migration: {types}")
    conn.close()

    shutil.rmtree(EXPORT_DIR)
    print("Done.")


if __name__ == "__main__":
    main()

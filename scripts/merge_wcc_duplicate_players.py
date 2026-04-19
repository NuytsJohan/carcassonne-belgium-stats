"""Merge WCC-imported foreign players into pre-existing DB records.

The WCC import (scripts/import_wk_results.py) looked up non-Belgian players
by `LOWER(name)` only. That missed pre-existing rows where the BGA handle
is stored as `name` and the real-world name as `name_nl` — producing
duplicate player rows for 18 foreigners.

Fix: for each duplicate (WCC row created with id >= 26405 whose `name`
matches an older row's `name_nl`), re-point the WCC tournament_participants
to the older (canonical) player_id and delete the duplicate.

Re-runnable: does nothing after the first successful merge.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
WCC_ID_START = 26405  # first id created by import_wk_results.py


def main():
    conn = duckdb.connect(str(DB_PATH))

    dups = conn.execute(
        """
        SELECT p1.id AS wcc_id, p2.id AS canonical_id,
               p1.name AS real_name, p2.name AS bga_name
        FROM players p1
        JOIN players p2
          ON LOWER(p2.name_nl) = LOWER(p1.name)
         AND p1.id <> p2.id
        WHERE p1.id >= ?
        ORDER BY p1.id
        """,
        [WCC_ID_START],
    ).fetchall()

    if not dups:
        print("No duplicates found; nothing to merge.")
        conn.close()
        return

    for wcc_id, canon_id, real, bga in dups:
        # Move tournament_participants rows over.
        moved = conn.execute(
            "UPDATE tournament_participants SET player_id = ? "
            "WHERE player_id = ? RETURNING id",
            [canon_id, wcc_id],
        ).fetchall()

        # Drop any alias rows that were attached to the duplicate (they're
        # redundant: the canonical player already stores the BGA handle as
        # `name`).
        aliases = conn.execute(
            "DELETE FROM player_aliases WHERE player_id = ? RETURNING alias",
            [wcc_id],
        ).fetchall()

        conn.execute("DELETE FROM players WHERE id = ?", [wcc_id])
        print(
            f"merged id={wcc_id} ({real!r}) -> id={canon_id} ({bga!r}); "
            f"{len(moved)} tp rows moved, {len(aliases)} aliases dropped"
        )

    print(f"\nTotal duplicates merged: {len(dups)}")
    conn.close()


if __name__ == "__main__":
    main()

"""Attach known BGA handles to foreign WCC players.

Follows the convention already used for Belgian players:
  players.name    = BGA handle
  players.name_nl = real-world full name

WCC xlsx imports only had the real name, so `name = real_name` and
`name_nl = NULL`. This script swaps them when we learn the BGA handle.

Re-runnable: skips a row that's already in the target shape.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

# (real-world full name, BGA handle)
ALIASES = [
    ("Daniel Angelats", "senglar"),
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    for real_name, bga in ALIASES:
        # Find by either current shape (name=real) or already-swapped (name_nl=real).
        row = conn.execute(
            "SELECT id, name, name_nl FROM players "
            "WHERE LOWER(name) = LOWER(?) OR LOWER(name_nl) = LOWER(?)",
            [real_name, real_name],
        ).fetchone()
        if not row:
            print(f"[skip] player not found: {real_name!r}")
            continue
        pid, cur_name, cur_nl = row
        if cur_name == bga and cur_nl == real_name:
            print(f"[ok]   {real_name!r} already has name={bga!r}")
            continue
        conn.execute(
            "UPDATE players SET name = ?, name_nl = ? WHERE id = ?",
            [bga, real_name, pid],
        )
        print(f"[upd]  id={pid}: name={bga!r}, name_nl={real_name!r}")

        # Drop an obsolete player_aliases row if we previously added one.
        removed = conn.execute(
            "DELETE FROM player_aliases WHERE alias = ? AND player_id = ? "
            "RETURNING alias",
            [bga, pid],
        ).fetchone()
        if removed:
            print(f"       dropped stale alias row {bga!r}")
    conn.close()


if __name__ == "__main__":
    main()

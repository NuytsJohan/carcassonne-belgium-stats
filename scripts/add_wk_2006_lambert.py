"""Add Christophe Lambert (BE) as second Belgian participant at WK 2006.

Source data (data/raw/carcassonne_wc_results.xlsx) had no Belgians for 2006.
User confirmed a second BE participant: Christophe Lambert, rank 12.
Creates the player record if needed and inserts the WK 2006 row.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TOURNAMENT_ID = 100          # WK 2006
PLAYER_NAME = "Christophe Lambert"
FINAL_RANK = 12


def main():
    conn = duckdb.connect(str(DB_PATH))

    row = conn.execute(
        "SELECT id FROM players "
        "WHERE LOWER(COALESCE(name_nl, name)) = LOWER(?)",
        [PLAYER_NAME],
    ).fetchone()
    if row:
        player_id = row[0]
        print(f"Matched existing player id={player_id}.")
    else:
        player_id = conn.execute(
            "INSERT INTO players (name, name_nl, country) "
            "VALUES (?, ?, 'BE') RETURNING id",
            [PLAYER_NAME, PLAYER_NAME],
        ).fetchone()[0]
        print(f"Created player id={player_id} ({PLAYER_NAME}, BE).")

    existing = conn.execute(
        "SELECT id FROM tournament_participants "
        "WHERE tournament_id = ? AND player_id = ?",
        [TOURNAMENT_ID, player_id],
    ).fetchone()
    if existing:
        print(f"Participation row already exists (id={existing[0]}); aborting.")
        conn.close()
        return

    next_id = conn.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM tournament_participants"
    ).fetchone()[0]

    conn.execute(
        "INSERT INTO tournament_participants "
        "(id, tournament_id, player_id, final_rank) "
        "VALUES (?, ?, ?, ?)",
        [next_id, TOURNAMENT_ID, player_id, FINAL_RANK],
    )
    print(
        f"Inserted WK 2006 participation for {PLAYER_NAME} "
        f"(rank={FINAL_RANK}, tp_id={next_id})."
    )
    conn.close()


if __name__ == "__main__":
    main()

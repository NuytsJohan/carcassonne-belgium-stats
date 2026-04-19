"""One-shot fix: add Johan Nuyts' missing WK 2006 participation.

Source data (data/raw/carcassonne_wc_results.xlsx) only had Belgians from
2007 onward, but Johan Nuyts (obiwonder) participated in the inaugural 2006
edition. Inserts a single tournament_participants row for WK 2006.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TOURNAMENT_ID = 100          # WK 2006
PLAYER_ID = 33               # Johan Nuyts (name='obiwonder', name_nl='Johan Nuyts')
FINAL_RANK = 9
NOTE = "Eerste Belgische WK-deelname"


def main():
    conn = duckdb.connect(str(DB_PATH))
    existing = conn.execute(
        "SELECT id FROM tournament_participants "
        "WHERE tournament_id = ? AND player_id = ?",
        [TOURNAMENT_ID, PLAYER_ID],
    ).fetchone()
    if existing:
        print(f"Row already exists (id={existing[0]}); aborting.")
        conn.close()
        return

    next_id = conn.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM tournament_participants"
    ).fetchone()[0]

    conn.execute(
        "INSERT INTO tournament_participants "
        "(id, tournament_id, player_id, final_rank, note) "
        "VALUES (?, ?, ?, ?, ?)",
        [next_id, TOURNAMENT_ID, PLAYER_ID, FINAL_RANK, NOTE],
    )
    print(
        f"Inserted WK 2006 participation for player_id={PLAYER_ID} "
        f"(rank={FINAL_RANK}, id={next_id})."
    )
    conn.close()


if __name__ == "__main__":
    main()

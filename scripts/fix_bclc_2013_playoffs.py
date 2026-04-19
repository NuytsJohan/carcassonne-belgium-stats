"""BCLC 2013 playoff score fill-in + QF corrections."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 21

# (stage, match_number, score_1, score_2). Player order must match existing rows.
UPDATES = [
    ("1/4 Finals",       1, 143, 63),   # Dieter vs Roland (Roland 117 -> 63)
    ("1/4 Finals",       2, 116, 115),  # Willem vs Simon (Simon 87 -> 115)
    ("1/4 Finals",       3, 117, 92),   # Peter vs Nele (Nele 115 -> 92)
    # QF M4 Hans 110-105 Wolf unchanged
    ("Loser Bracket R1", 1, 117, 87),   # Roland vs Simon
    ("Loser Bracket R1", 2, 126, 115),  # Wolf vs Nele
    ("1/2 Finals",       1, 147, 124),  # Dieter vs Willem
    ("1/2 Finals",       2, 100, 67),   # Peter vs Hans
    ("Final",            1, 98, 91),    # Dieter vs Peter
    ("3rd Place",        1, 106, 100),  # Hans vs Willem
    ("5th Place",        1, 113, 93),   # Wolf vs Roland
    ("7th Place",        1, 106, 104),  # Nele vs Simon
]


def main():
    conn = duckdb.connect(str(DB_PATH))

    for stage, mnum, s1, s2 in UPDATES:
        row = conn.execute(
            """
            SELECT tm.id, tm.game_id_1, tm.player_1_id, tm.player_2_id
            FROM tournament_matches tm
            WHERE tm.tournament_id = ? AND tm.stage = ? AND tm.match_number = ?
            """,
            [TID, stage, mnum],
        ).fetchone()
        assert row, f"Missing row: {stage} M{mnum}"
        tm_id, gid, p1_id, p2_id = row

        conn.execute(
            "UPDATE tournament_matches SET score_1 = ?, score_2 = ? WHERE id = ?",
            [s1, s2, tm_id],
        )
        conn.execute(
            "UPDATE game_players SET score = ? WHERE game_id = ? AND player_id = ?",
            [s1, gid, p1_id],
        )
        conn.execute(
            "UPDATE game_players SET score = ? WHERE game_id = ? AND player_id = ?",
            [s2, gid, p2_id],
        )
        print(f"  [{stage:20s} #{mnum}]  {s1} - {s2}  (tm_id={tm_id}, game={gid})")

    conn.close()


if __name__ == "__main__":
    main()

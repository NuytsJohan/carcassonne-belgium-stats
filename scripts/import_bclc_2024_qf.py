"""BCLC 2024 - 1/4 Finals + Loser Bracket R1.

Note: LB R1 M2 shows Wannes Vansina's score as blank in the source; stored NULL.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 30

MATCHES = [
    ("1/4 Finals", 1, "Nicolas Victor",      107, "Lorenzo Van Herrewege",  82, "1"),
    ("1/4 Finals", 2, "Nico Wellemans",       97, "Fabian Mouton",          91, "1"),
    ("1/4 Finals", 3, "Niels Ongena",        109, "Bart Dejaegher",         94, "1"),
    ("1/4 Finals", 4, "Kenny Forrez",         78, "Jochen Keymeulen",       82, "2"),

    ("Loser Bracket R1", 1, "Harlinde Dewulf",     98, "Maud Quaniers",         101, "2"),
    ("Loser Bracket R1", 2, "Jeroen Smolders",     82, "Wannes Vansina",       None, "1"),
    ("Loser Bracket R1", 3, "Raf Mesotten",       104, "Christine Coulon",       74, "1"),
    ("Loser Bracket R1", 4, "Tania Vanderaerden",  99, "Kristof Kesteloot",     113, "2"),
]


def pid(conn, name_nl: str) -> int:
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [name_nl]
    ).fetchone()
    assert row, f"Missing player: {name_nl}"
    return row[0]


def main():
    conn = duckdb.connect(str(DB_PATH))

    for stage in {m[0] for m in MATCHES}:
        c = conn.execute(
            "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id=? AND stage=?",
            [TID, stage],
        ).fetchone()[0]
        if c:
            print(f"{stage} already has {c} rows for tid={TID}; aborting.")
            conn.close()
            return

    max_tm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_matches").fetchone()[0]
    next_tm = max_tm + 1

    for stage, mnum, p1, s1, p2, s2, result in MATCHES:
        a_pid = pid(conn, p1)
        b_pid = pid(conn, p2)
        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TID, 2, mnum, "manual", "2024-01-01"],
        ).fetchone()[0]

        a_rank = 1 if result == "1" else 2
        b_rank = 2 if result == "1" else 1
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
            [gid, a_pid, s1, a_rank],
        )
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
            [gid, b_pid, s2, b_rank],
        )
        conn.execute(
            """
            INSERT INTO tournament_matches
                (id, tournament_id, stage, match_number, player_1_id, player_2_id,
                 score_1, score_2, result, notes, game_id_1)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
            """,
            [next_tm, TID, stage, mnum, a_pid, b_pid, s1, s2, result, gid],
        )
        next_tm += 1
        print(f"  [{stage} #{mnum}] {p1} {s1} - {s2} {p2}  result={result}  game_id={gid}")

    gcnt = conn.execute("SELECT COUNT(*) FROM games WHERE tournament_id=?", [TID]).fetchone()[0]
    tmcnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id=?", [TID]
    ).fetchone()[0]
    print(f"\nGames: {gcnt}, tournament_matches: {tmcnt}")

    conn.close()


if __name__ == "__main__":
    main()

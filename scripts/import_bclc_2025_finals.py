"""BCLC 2025 - Placement finals (1st/3rd/5th/7th)."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 31

MATCHES = [
    ("Final",     1, "Johan Nuyts",       124, "Raf Mesotten",    132, "2"),
    ("3rd Place", 1, "Nico Verlinden",     97, "Joren De Ridder",  96, "1"),
    ("5th Place", 1, "Thomas Declerck",    86, "Wannes Vansina",   79, "1"),
    ("7th Place", 1, "Andry Caluw\u00e9", 120, "Nicolas Rousseaux",100, "1"),
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
            [TID, 3, mnum, "manual", "2025-01-01"],
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
        print(f"  [{stage}] {p1} {s1} - {s2} {p2}  result={result}  game_id={gid}")

    conn.close()


if __name__ == "__main__":
    main()

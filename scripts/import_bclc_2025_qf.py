"""BCLC 2025 - 1/4 Finals (8-player playoff)."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 31
STAGE = "1/4 Finals"

MATCHES = [
    (1, "Johan Nuyts",       68, "Nicolas Rousseaux",    63, "1"),
    (2, "Nico Verlinden",    86, "Wannes Vansina",       65, "1"),
    (3, "Thomas Declerck",   70, "Joren De Ridder",      84, "2"),
    (4, "Raf Mesotten",     101, "Andry Caluw\u00e9",   100, "1"),
]


def pid(conn, name_nl: str) -> int:
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [name_nl]
    ).fetchone()
    assert row, f"Missing player: {name_nl}"
    return row[0]


def main():
    conn = duckdb.connect(str(DB_PATH))

    c = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id=? AND stage=?",
        [TID, STAGE],
    ).fetchone()[0]
    if c:
        print(f"{STAGE} already has {c} rows for tid={TID}; aborting.")
        conn.close()
        return

    max_tm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_matches").fetchone()[0]
    next_tm = max_tm + 1

    for mnum, p1, s1, p2, s2, result in MATCHES:
        a_pid = pid(conn, p1)
        b_pid = pid(conn, p2)
        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TID, 1, mnum, "manual", "2025-01-01"],
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
            [next_tm, TID, STAGE, mnum, a_pid, b_pid, s1, s2, result, gid],
        )
        next_tm += 1
        print(f"  [{STAGE} #{mnum}] {p1} {s1} - {s2} {p2}  result={result}  game_id={gid}")

    conn.close()


if __name__ == "__main__":
    main()

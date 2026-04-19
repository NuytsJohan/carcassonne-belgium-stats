"""BCLC 2023 - 1/4 Finals + Loser Bracket R1."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 29

MATCHES = [
    ("1/4 Finals", 1, "Wannes Vansina",   102, "Joren De Ridder",    72, "1"),
    ("1/4 Finals", 2, "Nicolas Victor",    92, "Raf Mesotten",       69, "1"),
    ("1/4 Finals", 3, "Renaud Godfraind", 110, "Patrick De Wilde",   83, "1"),
    ("1/4 Finals", 4, "Karl Verheyden",    95, "Koen Claeys",        52, "1"),

    ("Loser Bracket R1", 1, "Guy Comelis",         109, "Hans Vanden Bosch",  112, "2"),
    ("Loser Bracket R1", 2, "Stefan Meir",          49, "Simon Janssens",      47, "1"),
    ("Loser Bracket R1", 3, "Karel Demeersseman",   77, "Fabian Mouton",      134, "2"),
    ("Loser Bracket R1", 4, "Tim Van den Bosche",   99, "Robin Bastiaen",      98, "1"),
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
            [TID, 2, mnum, "manual", "2023-01-01"],
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

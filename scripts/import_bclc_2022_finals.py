"""BCLC 2022 - All placement finals (1st, 3rd, 5th, 7th, 9th, 11th, 13th, 15th)."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 28

MATCHES = [
    ("Final",       1, "Nicolas Victor",  100, "Raf Mesotten",                85, "1"),
    ("3rd Place",   1, "Joren De Ridder",  86, "Dominic R\u00e9b\u00e9rez",  101, "2"),
    ("5th Place",   1, "Tim Onghena",      75, "Bert Jacobs",                 79, "2"),
    ("7th Place",   1, "Els Smekens",     115, "Celien Neven",                70, "1"),
    ("9th Place",   1, "Niels Ongena",    105, "Nele De Pooter",             109, "2"),
    ("11th Place",  1, "Nico Wellemans",  107, "Bj\u00f6rn Petit",           100, "1"),
    ("13th Place",  1, "Kim Peeters",      95, "Philippe Kenens",             77, "1"),
    ("15th Place",  1, "Patrick De Wilde",109, "Geert Wouters",               89, "1"),
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
            [TID, 4, mnum, "manual", "2022-01-01"],
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

    gcnt = conn.execute("SELECT COUNT(*) FROM games WHERE tournament_id=?", [TID]).fetchone()[0]
    tmcnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id=?", [TID]
    ).fetchone()[0]
    print(f"\nGames: {gcnt}, tournament_matches: {tmcnt}")

    conn.close()


if __name__ == "__main__":
    main()

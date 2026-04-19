"""Add BCLC 2022 1/8 Finals (round of 16) games."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 28

# (match_number, p1, score1, p2, score2, result)
# result: '1' if p1 wins, '2' if p2 wins.
MATCHES = [
    (1, "Nicolas Victor",           90, "Patrick De Wilde",         73, "1"),
    (2, "Els Smekens",             108, "Niels Ongena",             97, "1"),
    (3, "Nico Wellemans",          100, "Joren De Ridder",         122, "2"),
    (4, "Kim Peeters",              92, "Tim Onghena",              97, "2"),
    (5, "Dominic R\u00e9b\u00e9rez", 117, "Geert Wouters",         116, "1"),
    (6, "Nele De Pooter",           97, "Celien Neven",            102, "2"),
    (7, "Raf Mesotten",             92, "Bj\u00f6rn Petit",          91, "1"),
    (8, "Philippe Kenens",          54, "Bert Jacobs",              66, "2"),
]

STAGE = "1/8 Finals"


def pid(conn, name_nl: str) -> int:
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [name_nl]
    ).fetchone()
    assert row, f"Missing player: {name_nl}"
    return row[0]


def main():
    conn = duckdb.connect(str(DB_PATH))

    existing = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND stage = ?",
        [TID, STAGE],
    ).fetchone()[0]
    if existing:
        print(f"{STAGE} already has {existing} rows for tid={TID}; aborting.")
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
            [TID, 1, mnum, "manual", "2022-01-01"],
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

    gcnt = conn.execute("SELECT COUNT(*) FROM games WHERE tournament_id = ?", [TID]).fetchone()[0]
    tmcnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    print(f"\nGames: {gcnt}, tournament_matches: {tmcnt}")

    conn.close()


if __name__ == "__main__":
    main()

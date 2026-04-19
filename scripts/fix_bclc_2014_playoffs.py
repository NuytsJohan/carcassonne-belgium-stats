"""BCLC 2014 playoff correction - wipe existing and reimport with correct scores/pairings."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 22

# (stage, match_number, round, p1, score1, p2, score2, result, notes)
MATCHES = [
    ("1/4 Finals",       1, 1, "Joren Peeters",      100, "Jonas Vereecken",      65,  "1", None),
    ("1/4 Finals",       2, 1, "Johan Nuyts",         94, "Pieter Verschraegen",  65,  "1", None),
    ("1/4 Finals",       3, 1, "Elien Vangheluwe",    57, "Simon Janssens",       57,  "1", "tiebreak (equal score)"),
    ("1/4 Finals",       4, 1, "Hans Baes",           96, "Willem Vansina",       71,  "1", None),

    ("Loser Bracket R1", 1, 2, "Jonas Vereecken",    106, "Pieter Verschraegen",  92,  "1", None),
    ("Loser Bracket R1", 2, 2, "Willem Vansina",      81, "Simon Janssens",       66,  "1", None),

    ("1/2 Finals",       1, 2, "Joren Peeters",      108, "Johan Nuyts",          89,  "1", None),
    ("1/2 Finals",       2, 2, "Hans Baes",           97, "Elien Vangheluwe",     84,  "1", None),

    ("Final",            1, 3, "Joren Peeters",       83, "Hans Baes",            65,  "1", None),
    ("3rd Place",        1, 3, "Johan Nuyts",        102, "Elien Vangheluwe",     88,  "1", None),
    ("5th Place",        1, 3, "Willem Vansina",     126, "Jonas Vereecken",     111,  "1", None),
    ("7th Place",        1, 3, "Simon Janssens",     109, "Pieter Verschraegen",  91,  "1", None),
]


def pid(conn, name_nl: str) -> int:
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [name_nl]
    ).fetchone()
    assert row, f"Missing player: {name_nl}"
    return row[0]


def main():
    conn = duckdb.connect(str(DB_PATH))

    # Wipe existing 2014 playoff
    game_ids = [r[0] for r in conn.execute(
        "SELECT DISTINCT game_id_1 FROM tournament_matches WHERE tournament_id=? AND game_id_1 IS NOT NULL",
        [TID],
    ).fetchall()]
    tm_count = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id=?", [TID]
    ).fetchone()[0]
    conn.execute("DELETE FROM tournament_matches WHERE tournament_id=?", [TID])
    if game_ids:
        qmarks = ",".join(["?"] * len(game_ids))
        conn.execute(f"DELETE FROM game_players WHERE game_id IN ({qmarks})", game_ids)
        conn.execute(f"DELETE FROM games WHERE id IN ({qmarks})", game_ids)
    print(f"Wiped {tm_count} tm rows and {len(game_ids)} games for tid={TID}.")

    max_tm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_matches").fetchone()[0]
    next_tm = max_tm + 1

    for stage, mnum, rnd, p1, s1, p2, s2, result, notes in MATCHES:
        a_pid = pid(conn, p1)
        b_pid = pid(conn, p2)
        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TID, rnd, mnum, "manual", "2014-01-01"],
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [next_tm, TID, stage, mnum, a_pid, b_pid, s1, s2, result, notes, gid],
        )
        next_tm += 1
        print(f"  [{stage:20s} #{mnum}] {p1} {s1} - {s2} {p2}  result={result}  game_id={gid}")

    conn.close()


if __name__ == "__main__":
    main()

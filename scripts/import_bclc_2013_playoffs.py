"""(Re)build BCLC 2013 playoff bracket.

- Deletes previously-inserted BCLC 2013 games / game_players / tournament_matches.
- Inserts all 12 playoff matches into `tournament_matches` with `stage` set.
- Creates a matching `games` row for each match (source='manual') and links it
  via tournament_matches.game_id_1.
- game_players rows capture scores + rank (1=winner, 2=loser).

Scores from the PDF are only confident for the 4 quarter-finals. For later rounds
we know the winner (from final rank) but not the score; we set `result` and
leave `score_1`/`score_2` NULL.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
TID = 21

# name_nl -> player_id (looked up below, but these are known to exist)
PLAYERS = [
    "Dieter Peeters",
    "Peter Nuyts",
    "Hans Gijd\u00e9",
    "Willem Van Bogaert",
    "Wolf Nuyts",
    "Roland Persoon",
    "Nele De Pooter",
    "Simon Janssens",
]

# Bracket structure.
# (stage, match_number, p1, p2, score_1, score_2, result)
#   result: '1' if p1 wins, '2' if p2 wins.
MATCHES = [
    # Quarter-finals (tables T1..T4)
    ("1/4 Finals", 1, "Dieter Peeters",     "Roland Persoon",   143, 117, "1"),
    ("1/4 Finals", 2, "Willem Van Bogaert", "Simon Janssens",   116,  87, "1"),
    ("1/4 Finals", 3, "Peter Nuyts",        "Nele De Pooter",   117, 115, "1"),
    ("1/4 Finals", 4, "Hans Gijd\u00e9",    "Wolf Nuyts",       110, 105, "1"),

    # Winners' bracket semi-finals (scores unknown; winners inferred from final ranks)
    ("1/2 Finals", 1, "Dieter Peeters",     "Willem Van Bogaert", None, None, "1"),
    ("1/2 Finals", 2, "Peter Nuyts",        "Hans Gijd\u00e9",    None, None, "1"),

    # Losers' bracket round 1 (QF losers pair up)
    ("Loser Bracket R1", 1, "Roland Persoon", "Simon Janssens", None, None, "1"),
    ("Loser Bracket R1", 2, "Wolf Nuyts",     "Nele De Pooter", None, None, "1"),

    # Medal / placement matches
    ("Final",      1, "Dieter Peeters",     "Peter Nuyts",        None, None, "1"),
    ("3rd Place",  1, "Hans Gijd\u00e9",    "Willem Van Bogaert", None, None, "1"),
    ("5th Place",  1, "Wolf Nuyts",         "Roland Persoon",     None, None, "1"),
    ("7th Place",  1, "Nele De Pooter",     "Simon Janssens",     None, None, "1"),
]

QF_TABLES = {("1/4 Finals", 1): 1, ("1/4 Finals", 2): 2,
             ("1/4 Finals", 3): 3, ("1/4 Finals", 4): 4}


def main():
    conn = duckdb.connect(str(DB_PATH))

    # --- Clean previous BCLC 2013 playoff rows ---
    conn.execute(
        "DELETE FROM game_players WHERE game_id IN (SELECT id FROM games WHERE tournament_id = ?)",
        [TID],
    )
    conn.execute("DELETE FROM games WHERE tournament_id = ?", [TID])
    conn.execute("DELETE FROM tournament_matches WHERE tournament_id = ?", [TID])
    print("Cleared previous BCLC 2013 games / tournament_matches.")

    # Resolve player_ids
    pid: dict[str, int] = {}
    for name in PLAYERS:
        row = conn.execute(
            "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [name]
        ).fetchone()
        assert row, f"Missing player name_nl: {name}"
        pid[name] = row[0]

    max_tm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_matches").fetchone()[0]
    next_tm_id = max_tm + 1

    for stage, mnum, a, b, s1, s2, result in MATCHES:
        a_pid, b_pid = pid[a], pid[b]
        table_no = QF_TABLES.get((stage, mnum))

        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TID, 1, table_no, "manual", "2013-01-01"],
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
            [next_tm_id, TID, stage, mnum, a_pid, b_pid, s1, s2, result, None, gid],
        )
        next_tm_id += 1
        print(f"  [{stage} #{mnum}] {a} {s1} - {s2} {b}  result={result}  game_id={gid}")

    gcnt = conn.execute("SELECT COUNT(*) FROM games WHERE tournament_id = ?", [TID]).fetchone()[0]
    tmcnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    print(f"\nGames: {gcnt}, tournament_matches: {tmcnt}")

    conn.close()


if __name__ == "__main__":
    main()

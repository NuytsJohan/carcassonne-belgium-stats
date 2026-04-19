"""Import BCLC 2013 final ranking + playoff quarter-final games.

Sources:
  - Page 2 'Eindklassement' for ranks (ignore page 1 round-by-round).
  - Page 3 'Eindronde' for the 4 QF playoff games.
Top-8 played playoffs and have no Pnt/Res on the Eindklassement, only rank.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 21
TOURNAMENT_NAME = "BCLC 2013"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2013

NAME_OVERRIDES = {
    # PDF 'Baes Jaak' in 2013 == earlier 'Jacques Baes' (short form of Jacques)
    "Jaak Baes": "Jacques Baes",
    # PDF 'Persoon Roland' matches existing normalized 'Roland Persoon'
    # (no override needed).
    # PDF 'Hacquaert Ellen' == earlier 'Hackaert Ellen' (2012). Keep as new
    # spelling distinct? Likely same person - keep as existing.
    "Ellen Hacquaert": "Ellen Hackaert",
    # PDF 'Spincemaille klaas' (lowercase k) - match existing 'Klaas Spincemaille'.
}

# (final_rank, name_nl, points_or_None, resistance_or_None)
PARTICIPANTS = [
    (1,  "Dieter Peeters",        None, None),
    (2,  "Peter Nuyts",           None, None),
    (3,  "Hans Gijd\u00e9",       None, None),
    (4,  "Willem Van Bogaert",    None, None),
    (5,  "Wolf Nuyts",            None, None),
    (6,  "Roland Persoon",        None, None),
    (7,  "Nele De Pooter",        None, None),
    (8,  "Simon Janssens",        None, None),
    (9,  "Johan Nuyts",           24, 0.2784),
    (10, "Lynn Hermie",           22, 0.2680),
    (11, "Elien Vangheluwe",      21, 0.2762),
    (12, "Joren Peeters",         20, 0.2838),
    (13, "Arnaud De Vuyst",       20, 0.2778),
    (14, "Nicolas Victor",        20, 0.2690),
    (15, "Karl Verheyden",        19, 0.2803),
    (16, "Maarten Vansina",       19, 0.2735),
    (17, "Pieter Verschraegen",   19, 0.2697),
    (18, "Marc Peeters",          19, 0.2596),
    (19, "Thijs Raman",           18, 0.2708),
    (20, "Nico Wellemans",        18, 0.2702),
    (21, "Willem Vansina",        18, 0.2622),
    (22, "Steven Van den Bosch",  18, 0.2609),
    (23, "Evelien Belmans",       18, 0.2558),
    (24, "Henk Detavernier",      18, 0.2504),
    (25, "Sylvia Van Dorpe",      18, 0.2436),
    (26, "Bruno De Lat",          17, 0.2627),
    (27, "Jaak Baes",             17, 0.2621),   # override → Jacques Baes
    (28, "Nick Leentjes",         17, 0.2536),
    (29, "Kevin Verelst",         17, 0.2465),
    (30, "Katrien Dellaert",      17, 0.2443),
    (31, "Kenny Forrez",          17, 0.2409),
    (32, "Wannes Vansina",        16, 0.2637),
    (33, "Melissa Verschueren",   16, 0.2583),
    (34, "Wim Veys",              16, 0.2543),
    (35, "Steven Vervaet",        16, 0.2429),
    (36, "Ellen Hacquaert",       16, 0.2410),   # override → Ellen Hackaert
    (37, "Klaas Spincemaille",    16, 0.2406),
    (38, "Bart Gerard",           15, 0.2649),
    (39, "Bart Descamps",         15, 0.2629),
    (40, "Simon Rutten",          15, 0.2472),
    (41, "Rita De Vos",           15, 0.2442),
    (42, "Bart Janssens",         15, 0.2401),
    (43, "Astrid Verschueren",    15, 0.2387),
    (44, "Josee Duchateau",       15, 0.2370),
    (45, "Kim Peeters",           14, 0.2578),
    (46, "Sophie Kemper",         14, 0.2467),
    (47, "Niels Munster",         14, 0.2410),
    (48, "Sven Wijnandt",         14, 0.2344),
    (49, "Jurgen Spreutels",      14, 0.2268),
    (50, "Kim Bassens",           14, 0.2239),
    (51, "Kevin Azijn",           13, 0.2586),
    (52, "Pascal Wellemans",      13, 0.2476),
    (53, "Koen Veys",             12, 0.2501),
    (54, "Peter Rombouts",        12, 0.2500),
    (55, "Els Smekens",           12, 0.2341),
    (56, "Wouter Florizoone",     12, 0.2240),
    (57, "Garry Forrez",          12, 0.2199),
    (58, "Franky Cnudde",         11, 0.2284),
    (59, "Pieter Vermeiren",       6, 0.2330),
]

# Quarter-final playoff games (round=1 in knockout; table T1..T4).
# Derived from page 3 'Eindronde' and cross-checked against final ranks:
# winners go to semis (-> ranks 1-4), losers go to 5-8 bracket (-> ranks 5-8).
# (table_number, player_a, score_a, player_b, score_b, winner_rank_of_a_or_b)
QF_GAMES = [
    # T1: Peeters 143 - Persoon 117 -> Peeters wins
    (1, "Dieter Peeters",     143, "Roland Persoon", 117, "a"),
    # T2: Van Bogaert 116 - Janssens 87 -> Van Bogaert wins
    (2, "Willem Van Bogaert", 116, "Simon Janssens",  87, "a"),
    # T3: P.Nuyts 117 - De Pooter 115 -> P.Nuyts wins
    (3, "Peter Nuyts",        117, "Nele De Pooter", 115, "a"),
    # T4: Gijdé 110 - W.Nuyts 105 -> Gijdé wins (Klass 4 > Klass 5)
    (4, "Hans Gijd\u00e9",    110, "Wolf Nuyts",     105, "a"),
]


def find_or_create_player(conn, name_nl: str) -> tuple[int, bool]:
    canonical = NAME_OVERRIDES.get(name_nl, name_nl)
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)",
        [canonical],
    ).fetchone()
    if row:
        return row[0], False
    new_id = conn.execute(
        "INSERT INTO players (name, name_nl, country) VALUES (?, ?, 'BE') RETURNING id",
        [canonical, canonical],
    ).fetchone()[0]
    return new_id, True


def main():
    conn = duckdb.connect(str(DB_PATH))

    if conn.execute("SELECT id FROM tournaments WHERE id = ?", [TOURNAMENT_ID]).fetchone():
        print(f"Tournament id={TOURNAMENT_ID} already exists; aborting.")
        conn.close()
        return

    conn.execute(
        """
        INSERT INTO tournaments (id, name, type, year, edition, date_start, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            TOURNAMENT_ID,
            TOURNAMENT_NAME,
            TOURNAMENT_TYPE,
            TOURNAMENT_YEAR,
            "2013",
            "2013-01-01",
            "Belgian Live Championship 2013 - Eindklassement from PDF; top 8 played a "
            "knockout playoff (no Pnt/Res shown).",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    pid_by_name: dict[str, int] = {}
    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, points, resistance in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        pid_by_name[canonical] = pid
        pid_by_name[name_nl] = pid
        (created if was_created else matched).append((pid, canonical))

        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, resistance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TOURNAMENT_ID, pid, rank, points, resistance],
        )
        next_tp_id += 1

    print(f"\nMatched existing players ({len(matched)}):")
    for pid, n in matched:
        print(f"  [{pid}] {n}")
    print(f"\nCreated new players ({len(created)}):")
    for pid, n in created:
        print(f"  [{pid}] {n}")

    # --- Playoff games ---
    for table_no, a_name, a_score, b_name, b_score, winner in QF_GAMES:
        a_pid = pid_by_name[a_name]
        b_pid = pid_by_name[b_name]
        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TOURNAMENT_ID, 1, table_no, "manual", "2013-01-01"],
        ).fetchone()[0]

        a_rank = 1 if winner == "a" else 2
        b_rank = 2 if winner == "a" else 1
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
            [gid, a_pid, a_score, a_rank],
        )
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
            [gid, b_pid, b_score, b_rank],
        )
        print(f"  QF T{table_no}: {a_name} {a_score} - {b_name} {b_score}  (game_id={gid})")

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?",
        [TOURNAMENT_ID],
    ).fetchone()[0]
    gcnt = conn.execute(
        "SELECT COUNT(*) FROM games WHERE tournament_id = ?", [TOURNAMENT_ID]
    ).fetchone()[0]
    print(f"\nTotal participants: {cnt}, games: {gcnt}")

    conn.close()


if __name__ == "__main__":
    main()

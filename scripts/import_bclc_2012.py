"""Import BCLC 2012 final ranking.

Source: BK Carcassonne 2012 Einduitslag.PDF (Klassement na RONDE 6).
Columns: Klass, Pnt, Res, I-Nr, Naam (Surname Firstname), Club.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 20
TOURNAMENT_NAME = "BCLC 2012"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2012

# Reconcile PDF spellings with existing player name_nl.
NAME_OVERRIDES = {
    "Roland Persoons": "Roland Persoon",
    "Mark Rattat": "Marc Rattat",  # PDF spelling 'Mark' vs earlier 'Marc'
}

# (final_rank, name_nl, points, resistance)
PARTICIPANTS = [
    (1,  "Bruno De Lat",           24, 0.2836),
    (2,  "Karl Verheyden",         24, 0.2808),
    (3,  "Sander Willems",         23, 0.2817),
    (4,  "Dirk Rattat",            22, 0.2733),
    (5,  "Marc Peeters",           22, 0.2448),
    (6,  "Ellen Hackaert",         21, 0.2850),
    (7,  "Kevin Verelst",          21, 0.2732),
    (8,  "Willem Vansina",         21, 0.2669),
    (9,  "Maarten Vansina",        20, 0.2834),
    (10, "Kim Peeters",            20, 0.2720),
    (11, "Wannes Vansina",         20, 0.2719),
    (12, "Steven Van den Bosch",   19, 0.2636),
    (13, "Klaas Spincemaille",     19, 0.2605),
    (14, "Dieter Peeters",         19, 0.2566),
    (15, "Rita De Vos",            19, 0.2523),
    (16, "Els Smekens",            18, 0.2766),
    (17, "Katrien Dellaert",       18, 0.2617),
    (18, "Wolf Nuyts",             18, 0.2603),
    (19, "Johan Nuyts",            18, 0.2572),
    (20, "Elien Vangheluwe",       18, 0.2503),
    (21, "Arnaud De Vuyst",        18, 0.2420),
    (22, "Simon Janssens",         17, 0.2627),
    (23, "Hans Baes",              17, 0.2505),
    (24, "Kim Bassens",             17, 0.2460),
    (25, "Sophie Kemper",          17, 0.2459),
    (26, "Pascal Wellemans",       17, 0.2374),
    (27, "Pieter Verschraegen",    16, 0.2588),
    (28, "Annelies De Smedt",      16, 0.2464),
    (29, "Nico Wellemans",         16, 0.2459),
    (30, "Maarten Bartolomees",    16, 0.2442),
    (31, "Anne Haesevoets",        16, 0.2437),
    (32, "Roland Persoons",        16, 0.2435),   # override
    (33, "Nick Leentjes",          15, 0.2497),
    (34, "Sylvia Van Dorpe",       15, 0.2460),
    (35, "Jurgen Spreutels",       15, 0.2411),
    (36, "Kevin Azijn",            15, 0.2296),
    (37, "Siebrecht Descamps",     14, 0.2689),
    (38, "Simon Rutten",           14, 0.2497),
    (39, "Joren Peeters",          14, 0.2343),
    (40, "Thijs Raman",            14, 0.2289),
    (41, "Melissa Verschueren",    14, 0.2289),
    (42, "Jacques Baes",           14, 0.2248),
    (43, "Bart Descamps",          13, 0.2559),
    (44, "Hans Gijd\u00e9",        13, 0.2543),
    (45, "Peter Nuyts",            13, 0.2250),
    (46, "Benjamin Symons",        12, 0.2345),
    (47, "Tom Haeck",              12, 0.2318),
    (48, "Jo Pirard",              11, 0.2347),
    (49, "Bj\u00f6rn Petit",       11, 0.2265),
    (50, "Josee Duchateau",        10, 0.2196),
    (51, "Mark Rattat",             9, 0.2261),   # override → Marc Rattat
    (52, "Lynn Hermie",             9, 0.2095),
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
            "2012",
            "2012-01-01",
            "Belgian Live Championship 2012 - final ranking after Round 6.",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, points, resistance in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
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

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?",
        [TOURNAMENT_ID],
    ).fetchone()[0]
    print(f"\nTotal participants for {TOURNAMENT_NAME}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

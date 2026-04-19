"""Import BCLC 2011 (Belgisch Kampioenschap Carcassonne) final ranking.

Source: BK Carcassonne 2011 - Einduitslag.pdf (Klassement na RONDE 5).
Columns: Klass (rank), Pnt, Res (resistance), I-Nr, Naam (Surname, Firstname).
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 19
TOURNAMENT_NAME = "BCLC 2011"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2011

# Manual overrides to reconcile PDF spellings with existing players.
# PDF 'Fourier Sylvia' is the same person as existing 'Sylvia Foerier' (id=40).
# PDF 'Persoons Roland' is the same person as existing 'Roland Persoon' (id=26063, 2010).
NAME_OVERRIDES = {
    "Sylvia Fourier": "Sylvia Foerier",
    "Roland Persoons": "Roland Persoon",
}

# (final_rank, name_nl (already normalised to 'Firstname Surname'), points, resistance)
PARTICIPANTS = [
    (1,  "Peter Nuyts",         21, 0.2852),
    (2,  "Nicolas Victor",      20, 0.3048),
    (3,  "Kim Peeters",         19, 0.2887),
    (4,  "Klaas Spincemaille",  19, 0.2748),
    (5,  "Karl Verheyden",      18, 0.2783),
    (6,  "Jurgen Spreutels",    18, 0.2631),
    (7,  "Elien Vangheluwe",    18, 0.2555),
    (8,  "Annelies Deridder",   17, 0.2792),
    (9,  "Henk Detavernier",    17, 0.2631),
    (10, "Nico Wellemans",      16, 0.2770),
    (11, "Kevin Azijn",         16, 0.2682),
    (12, "Rita De Vos",         16, 0.2640),
    (13, "Hans Gijdé",          16, 0.2595),
    (14, "Johan Nuyts",         15, 0.2681),
    (15, "Nick Leentjes",       15, 0.2661),
    (16, "Frederik De Lie",     15, 0.2633),
    (17, "Roland Persoons",     15, 0.2600),   # override → Roland Persoon
    (18, "Sylvia Fourier",      15, 0.2594),   # override → Sylvia Foerier
    (19, "Lynn Hermie",         15, 0.2464),
    (20, "Franky Cnudde",       14, 0.2639),
    (21, "Bart Descamps",       14, 0.2573),
    (22, "Marc Peeters",        14, 0.2510),
    (23, "Els Smekens",         14, 0.2505),
    (24, "Wolf Nuyts",          14, 0.2420),
    (25, "Pieter Vermeiren",    14, 0.2402),
    (26, "Bart Gerard",         13, 0.2525),
    (27, "Dirk Rattat",         13, 0.2401),
    (28, "Sylvia Van Dorpe",    12, 0.2691),
    (29, "Arnaud De Vuyst",     12, 0.2637),
    (30, "Steven van den Bosch",12, 0.2615),
    (31, "Dieter Peeters",      12, 0.2541),
    (32, "Pascal Wellemans",    12, 0.2305),
    (33, "Jurian Michiels",     11, 0.2413),
    (34, "Simon Janssens",      11, 0.2320),
    (35, "Kim Bassens",         10, 0.2566),
    (36, "Michael De Doncker",  10, 0.2564),
    (37, "Katrien Dellaert",    10, 0.2462),
    (38, "Pieter Verschraegen",  9, 0.2312),
    (39, "Marc Rattat",          9, 0.2169),
    (40, "Christof Jacques",     8, 0.2545),
    (41, "Leen Desopper",        7, 0.2511),
    (42, "Vito Nuyts",           7, 0.2148),
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
            "2011",
            "2011-01-01",
            "Belgian Live Championship 2011 - final ranking after Round 5.",
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

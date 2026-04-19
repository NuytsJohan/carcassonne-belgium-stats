"""Import BCLC 2010 (Belgisch Kampioenschap Carcassonne) final ranking.

Source: screenshot for BCLC 2010.
Columns: Klass (rank), Pnt (match points), Res (resistance), I-Nr (start nr), Naam.
Names are 'Surname Firstname'.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 18
TOURNAMENT_NAME = "BCLC 2010"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2010

# (final_rank, name_nl, points, resistance)
PARTICIPANTS = [
    (1,  "Peter Nuyts",         21, 0.3349),
    (2,  "Dirk Rattat",         18, 0.2769),
    (3,  "Katrien Dellaert",    17, 0.2806),
    (4,  "Roland Persoon",      17, 0.2780),
    (5,  "Christophe Lucas",    16, 0.2843),
    (6,  "Nico Wellemans",      16, 0.2557),
    (7,  "Sylvia Van Dorpe",    15, 0.2692),
    (8,  "Rita De Vos",         15, 0.2563),
    (9,  "Kris Boyen",          15, 0.2456),
    (10, "Jurian Michiels",     14, 0.2869),
    (11, "Lieve Peirtsegaele",  14, 0.2430),
    (12, "Winand Goyvaerts",    13, 0.2628),
    (13, "Bert Meynckels",      13, 0.2560),
    (14, "Arnaud De Vuyst",     13, 0.2454),
    (15, "Marc Peeters",        12, 0.2418),
    (16, "Sylvia Foerier",      11, 0.2482),
    (17, "Ineke Hoolants",      11, 0.2403),
    (18, "Marc Rattat",         11, 0.2379),
    (19, "Inez Schraepen",      10, 0.2560),
]


def find_or_create_player(conn, name_nl: str) -> tuple[int, bool]:
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)",
        [name_nl],
    ).fetchone()
    if row:
        return row[0], False
    new_id = conn.execute(
        "INSERT INTO players (name, name_nl, country) VALUES (?, ?, 'BE') RETURNING id",
        [name_nl, name_nl],
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
            "2010",
            "2010-01-01",
            "Belgian Live Championship 2010 - final ranking.",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, points, resistance in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        (created if was_created else matched).append((pid, name_nl))

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

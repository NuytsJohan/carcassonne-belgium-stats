"""Import BCLC 2008 (Belgisch Kampioenschap Carcassonne) final ranking.

Source: BK Carcassonne 2008_finale.pdf
PDF columns: NAAM, PUNTEN, %. No 'speelpunten' column this year.
Names mostly 'Surname Firstname'; exceptions: 'Tom Declercq' (first-first),
'van Everdingen Piet' (tussenvoegsel 'van Everdingen' + firstname 'Piet').
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 17
TOURNAMENT_NAME = "BCLC 2008"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2008

# (final_rank, name_nl, match_points, win_pct)
PARTICIPANTS = [
    (1,  "Peter Nuyts",          15, 0.87),
    (2,  "Johan Nuyts",          12, 0.94),
    (3,  "Kristof Lerno",        11, 0.92),
    (4,  "Wouter Goris",         11, 0.84),
    (5,  "Danny Coenen",         11, 0.82),
    (6,  "Alexander Bruggeman",  11, 0.81),
    (7,  "Debby De Gendt",       11, 0.78),
    (8,  "Rafael Robberechts",   11, 0.73),
    (9,  "Rudy De Weerdt",       11, 0.72),
    (10, "Tom Declercq",         10, 0.83),
    (11, "Wim Veys",             10, 0.81),
    (12, "Sam Faes",              10, 0.79),
    (13, "Wannes Vansina",       10, 0.78),
    (14, "Kirsten Van Camp",      9, 0.83),
    (15, "Frederik De Lie",       9, 0.82),
    (16, "Johan Huyck",           9, 0.80),
    (17, "Anne Smeyers",          8, 0.73),
    (18, "Nicolas Victor",        7, 0.77),
    (19, "Didier De Breuck",      7, 0.75),
    (20, "Chris Ranschaert",      7, 0.73),
    (21, "Els Smekens",           6, 0.74),
    (22, "Nico De Bleye",         6, 0.71),
    (23, "Wolf Nuyts",            6, 0.71),
    (24, "Martine Goddeeris",     6, 0.71),
    (25, "Wesley Herpoel",        6, 0.69),   # PDF typo 'Wesly'
    (26, "Karl Verheyden",        6, 0.61),
    (27, "Sandra De Cooman",      5, 0.65),
    (28, "Werner Christiaens",    5, 0.58),
    (29, "Tom Geukens",           4, 0.70),
    (30, "Stijn Descamps",        4, 0.65),
    (31, "Piet van Everdingen",   4, 0.62),
    (32, "Maaike Peene",          3, 0.56),
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
            "2008",
            "2008-01-01",
            "Belgian Live Championship 2008 - final ranking from BK Carcassonne 2008_finale.pdf.",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, match_points, win_pct in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        (created if was_created else matched).append((pid, name_nl))

        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, win_pct)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TOURNAMENT_ID, pid, rank, match_points, win_pct],
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

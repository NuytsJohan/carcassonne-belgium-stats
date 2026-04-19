"""Import BCLC 2007 (Belgisch Kampioenschap Carcassonne) final ranking.

Source: BK Carcassonne 2007_finale.pdf
PDF columns: NAAM, speelpunten (game score), punten (match points), % (win pct).
Names in PDF are 'Surname Firstname'.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 16
TOURNAMENT_NAME = "BCLC 2007"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2007

# (final_rank, name_nl, total_score_speelpunten, match_points, win_pct)
PARTICIPANTS = [
    (1,  "Johan Nuyts",          207, 15, 0.88),
    (2,  "Frederik De Lie",      195, 15, 0.88),
    (3,  "Nele Claerhout",       205, 13, 0.81),
    (4,  "Nicolas Victor",       220, 12, 0.89),
    (5,  "Els Smekens",          186, 12, 0.81),
    (6,  "Karl Verheyden",       226, 11, 0.83),
    (7,  "Danny Coenen",         217, 11, 0.80),
    (8,  "Hendrik Pisman",       190, 10, 0.90),
    (9,  "Raf Colson",           197, 10, 0.86),
    (10, "Nico De Bleye",        208, 10, 0.84),
    (11, "Pieter Verbert",       184, 10, 0.79),
    (12, "Dirk Rattat",          190, 10, 0.78),
    (13, "Dirk Struyf",          192,  9, 0.81),
    (14, "Wesley Herpoel",       168,  9, 0.72),
    (15, "Wouter Goris",         186,  8, 0.80),
    (16, "Joery De Weerdt",      212,  8, 0.74),
    (17, "Laurent Mataigne",     166,  8, 0.72),
    (18, "Rafael Robberechts",   196,  7, 0.73),
    (19, "Bart Vanneste",        192,  7, 0.72),
    (20, "Kristel Gouverneur",   186,  7, 0.72),
    (21, "Roel Van Doorsselaer", 175,  7, 0.71),
    (22, "Kirsten Picavet",      148,  7, 0.62),
    (23, "Paul Peeters",         198,  6, 0.74),
    (24, "Rudy De Weerdt",       192,  6, 0.71),
    (25, "Wannes Vansina",       154,  6, 0.71),
    (26, "Ward De Pauw",         161,  6, 0.68),
    (27, "Korneel Marchand",     162,  6, 0.67),
    (28, "Thijs Metz",           143,  6, 0.64),
    (29, "Koen Vandeneede",      151,  5, 0.66),
    (30, "Sandra De Cooman",     160,  5, 0.62),
    (31, "Kris Van Gucht",       135,  4, 0.56),
    (32, "Kristof Magchiels",    160,  3, 0.64),
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
            "2007",
            "2007-01-01",
            "Belgian Live Championship 2007 - final ranking from BK Carcassonne 2007_finale.pdf.",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, speelpunten, match_points, win_pct in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        (created if was_created else matched).append((pid, name_nl))

        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, total_score, points, win_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TOURNAMENT_ID, pid, rank, speelpunten, match_points, win_pct],
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

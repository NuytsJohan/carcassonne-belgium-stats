"""Import BCLC 2025 Eindklassement + top-8 playoff ranking.

54 participants; top 8 played playoffs (final rank 1-8 from playoff outcome),
9-54 from Eindklassement (points/resistance retained for all rows as printed
in source).
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 31
TNAME = "BCLC 2025"
TTYPE = "BCLC"
TYEAR = 2025

NAME_OVERRIDES = {
    "John Vanhees": "John Van Hees",
    "Lorenzo Van herrewege": "Lorenzo Van Herrewege",
    "Guy Cornelis": "Guy Comelis",
    "Johan Van der Wal": "Johan Van Der Wal",
    "Tim Ongena": "Tim Onghena",
    "Karolien Thys": "Karolien Thijs",
}

# (rank, name_nl, points, resistance). Ranks 1-8 = playoff order.
PLAYOFF_TOP8 = [
    (1, "Raf Mesotten",        5, 3.217607),
    (2, "Johan Nuyts",         5, 3.303306),
    (3, "Nico Verlinden",      5, 3.208124),
    (4, "Joren De Ridder",     4, 3.236554),
    (5, "Thomas Declerck",     5, 3.381714),
    (6, "Wannes Vansina",      5, 3.148398),
    (7, "Andry Caluw\u00e9",   5, 3.096960),
    (8, "Nicolas Rousseaux",   4, 3.233021),
]

# Ranks 9..54 from Eindklassement.
TAIL = [
    ("Wolf Nuyts",              4, 3.228886),
    ("Nicolas Victor",          4, 3.222382),
    ("Karl Verheyden",          4, 3.167718),
    ("Nico Wellemans",          4, 3.126389),
    ("Tom De Smedt",            4, 3.094003),
    ("Lieselotte De Vuyst",     4, 3.038857),
    ("Thomas Mackay",           4, 3.030599),
    ("Walther Mackay",          4, 3.014030),
    ("Johanna Dumon",           4, 2.955200),
    ("Joanne Mackay",           4, 2.934669),
    ("Agnetha Van Herrewege",   4, 2.915651),
    ("Patrick De Wilde",        3, 3.279608),
    ("John Vanhees",            3, 3.160905),   # -> John Van Hees
    ("Lorenzo Van herrewege",   3, 3.111535),   # -> Lorenzo Van Herrewege
    ("Michiel Meire",           3, 3.097066),
    ("Fabian Mouton",           3, 3.079107),
    ("Michiel Beerden",         3, 3.044143),
    ("Dominic R\u00e9b\u00e9rez", 3, 3.030600),
    ("Karel Demeersseman",      3, 2.997458),
    ("Kjell Dermul",            3, 2.990850),
    ("Dries Fillet",            3, 2.973051),
    ("Byron Lemoine",           3, 2.958120),
    ("Guy Cornelis",            3, 2.901454),   # -> Guy Comelis
    ("Bart Peeters",            3, 2.872807),
    ("Hans Scheers",            3, 2.863959),
    ("Johan Van der Wal",       3, 2.862323),   # -> Johan Van Der Wal
    ("Louis Keusters",          3, 2.744568),
    ("Tania Vanderaerden",      2, 3.004685),
    ("Michiel Demeersseman",    2, 2.980632),
    ("Christine Coulon",        2, 2.967321),
    ("Emmerick Daubie",         2, 2.954185),
    ("Geert Wouters",           2, 2.948970),
    ("Rein Lambrichts",         2, 2.917119),
    ("Tim Ongena",              2, 2.902476),   # -> Tim Onghena
    ("Christophe Keusters",     2, 2.888564),
    ("Gilles Verbeken",         2, 2.879680),
    ("Ron Jongeneel",           2, 2.879349),
    ("Harlinde Dewulf",         2, 2.837769),
    ("Bernard Peeters",         2, 2.824902),
    ("Pieter De Vuyst",         2, 2.784315),
    ("Marissa Sorgeloos",       1, 2.924661),
    ("Christophe Heylen",       1, 2.854349),
    ("Jasper Ongena",           1, 2.853293),
    ("Karolien Thys",           1, 2.785881),   # -> Karolien Thijs
    ("Fien Wellemans",           1, 2.767925),
    ("Alvar De Vuyst",          0, 2.522714),
]


def find_or_create_player(conn, name_nl: str) -> tuple[int, bool]:
    canonical = NAME_OVERRIDES.get(name_nl, name_nl)
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [canonical]
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

    if conn.execute("SELECT id FROM tournaments WHERE id = ?", [TID]).fetchone():
        print(f"Tournament id={TID} already exists; aborting.")
        conn.close()
        return

    conn.execute(
        """
        INSERT INTO tournaments (id, name, type, year, edition, date_start, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [TID, TNAME, TTYPE, TYEAR, "2025", "2025-01-01",
         "Belgian Live Championship 2025 - 54 participants; top 8 played playoff "
         "(final rank 1-8), 9-54 from Eindklassement."],
    )
    print(f"Created {TNAME} (id={TID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []

    for rank, name_nl, pts, res in PLAYOFF_TOP8:
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))
        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, resistance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TID, pid, rank, pts, res],
        )
        next_tp_id += 1

    for offset, (name_nl, pts, res) in enumerate(TAIL):
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))
        final_rank = 9 + offset
        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, resistance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TID, pid, final_rank, pts, res],
        )
        next_tp_id += 1

    print(f"\nMatched existing: {len(matched)} | Created new: {len(created)}")
    for pid, n in created:
        print(f"  NEW [{pid}] {n}")

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    print(f"\nParticipants: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

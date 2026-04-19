"""Import BCLC 2023 final ranking + top-16 playoff.

Source: two screenshots.
- Eindklassement after round 5: 82 participants with Tornooi (points; can be
  half-integers) and Weerst (resistance, 2022-style scale >1).
- Top 16 played a playoff; final playoff rank from second image.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 29
TNAME = "BCLC 2023"
TTYPE = "BCLC"
TYEAR = 2023

NAME_OVERRIDES = {
    "Guy Cornelis": "Guy Comelis",
    "Tim Van Den Bossche": "Tim Van den Bosche",
    "John Vanhees": "John Van Hees",
    "Johan Van der Wal": "Johan Van Der Wal",
    "Tim Ongena": "Tim Onghena",
    "Raf Michels": "Raf Michiels",
    "Yoeri Blomme": "Youri Blomme",
    "Lieselotte Devuyst": "Lieselotte De Vuyst",
    "Karolien Thys": "Karolien Thijs",
}

# Eindklassement: (name_nl, points, resistance)
EINDKLASSEMENT = [
    ("Wannes Vansina",             25,   2.945405),
    ("Karel Demeersseman",         25,   2.835184),
    ("Karl Verheyden",             23,   2.984666),
    ("Nicolas Victor",             23,   2.887053),
    ("Raf Mesotten",               22,   2.862699),
    ("Koen Claeys",                22,   2.890067),
    ("Fabian Mouton",              22,   2.86816),
    ("Hans Vanden Bosch",          22,   2.850094),
    ("Joren De Ridder",            21,   2.874345),
    ("Patrick De Wilde",           21,   2.851736),
    ("Robin Bastiaen",             21,   2.840023),
    ("Simon Janssens",             21,   2.829441),
    ("Stefan Meir",                21,   2.822208),
    ("Tim Van Den Bossche",        21,   2.776686),   # -> Tim Van den Bosche
    ("Renaud Godfraind",           21,   2.770255),
    ("Guy Cornelis",               21,   2.754573),   # -> Guy Comelis
    ("Nico Verlinden",             20,   1.325753),
    ("Kevin De Pelsmaker",         19,   1.347426),
    ("Lorenzo Van Herrewege",      17,   1.386256),
    ("Johan Nuyts",                17,   1.367742),
    ("Geert Wouters",              17,   1.33083),
    ("Niels Ongena",               17,   1.25219),
    ("Joachim Deschuytter",        16,   1.380471),
    ("Arnaud De Vuyst",            16,   1.358259),
    ("Tom De Smedt",               16,   1.354353),
    ("Bertrand Hallyn",            16,   1.323572),
    ("John Vanhees",               16,   1.316972),   # -> John Van Hees
    ("Tom Van Herrewege",          16,   1.297694),
    ("An Van Der Goten",           16,   1.330352),
    ("Nicky Hendriks",             15,   1.31471),
    ("Christophe Heylen",          15,   1.313976),
    ("Kenny Forrez",               15,   1.304497),
    ("Maud Quaniers",              15,   1.291775),
    ("Agnetha Van Herrewege",      15,   1.27815),
    ("Arno Vanthienen",            15,   1.254123),
    ("Johan Van der Wal",          14.5, 1.28732),    # -> Johan Van Der Wal
    ("Christine Coulon",           14.5, 1.226217),
    ("An Delhaye",                 14,   1.306702),
    ("Nico Wellemans",             14,   1.306191),
    ("Dominic R\u00e9b\u00e9rez",  14,   1.304872),
    ("Davy Forrez",                14,   1.258229),
    ("Jochen Keymeulen",           14,   1.226122),
    ("Gerrit Broeders",            14,   1.219418),
    ("Mieke Suenens",              14,   1.206593),
    ("Vital Pluymers",             13,   1.319809),
    ("Bernard Peeters",            13,   1.269694),
    ("Nele De Pooter",             13,   1.242487),
    ("Stephen Fleerackers",        13,   1.190079),
    ("Tania Vanderaerden",         13,   1.190052),
    ("Lieselotte Devuyst",         13,   1.187491),   # -> Lieselotte De Vuyst
    ("Davy Remans",                13,   1.186017),
    ("Sophie Peeters",             13,   1.182001),
    ("Kristof Kesteloot",          12.5, 1.313186),
    ("Yoeri Blomme",               12.5, 1.265985),   # -> Youri Blomme
    ("Dave Segers",                12,   1.287734),
    ("Johanna Dumon",              12,   1.271203),
    ("Tim Ongena",                 12,   1.235551),   # -> Tim Onghena
    ("Arne Vanstechelman",         12,   1.231283),
    ("Michiel Beerden",            12,   1.212695),
    ("Walther Mackay",             12,   1.204215),
    ("Nathan Declerck",            12,   1.168753),
    ("Nathalie Vanden Dorpe",      12,   1.162254),
    ("Frederick Van Hulle",        11,   1.25671),
    ("Raf Michels",                11,   1.244986),   # -> Raf Michiels
    ("Marissa Sorgeloos",          11,   1.226631),
    ("Britt Hendrickx",            11,   1.193301),
    ("Anthony Reynolds",           11,   1.177528),
    ("Andry Caluw\u00e9",          11,   1.158754),
    ("Jasper Ongena",              11,   1.155633),
    ("Brecht Volders",             11,   1.144382),
    ("Tea Djogic",                 11,   1.089498),
    ("Jolien De Schaepmeester",    10,   1.239643),
    ("Bjorn Van Vreckem",          10,   1.130806),
    ("Sylvia Foerier",             10,   1.106156),
    ("Jess Hendrickx",             10,   1.094081),
    ("Gil Goens",                  10,   1.060172),
    ("Kurt Dierickx",              10,   0.955983),
    ("Els Smekens",                 9,   1.145454),
    ("Karolien Thys",               9,   1.031759),   # -> Karolien Thijs
    ("Chesney Van Puymbroek",       9,   0.997026),
    ("Cindy Vandoorne",             9,   0.930935),
    ("Astrid Bodyn",                9,   0.926709),
]

# Playoff final ranks 1..16 (name spelling matches Eindklassement input).
PLAYOFF_RANKS = [
    "Karl Verheyden",         # 1
    "Wannes Vansina",         # 2
    "Nicolas Victor",         # 3
    "Renaud Godfraind",       # 4
    "Raf Mesotten",           # 5
    "Koen Claeys",            # 6
    "Joren De Ridder",        # 7
    "Patrick De Wilde",       # 8
    "Fabian Mouton",          # 9
    "Stefan Meir",            # 10
    "Hans Vanden Bosch",      # 11
    "Tim Van Den Bossche",    # 12
    "Simon Janssens",         # 13
    "Karel Demeersseman",     # 14
    "Guy Cornelis",           # 15
    "Robin Bastiaen",         # 16
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
        [TID, TNAME, TTYPE, TYEAR, "2023", "2023-01-01",
         "Belgian Live Championship 2023 - 82 participants; Eindklassement after "
         "round 5. Top 16 played playoff (final rank 1-16); 17-82 from "
         "Eindklassement. Pnt/Res use 2022+ scoring."],
    )
    print(f"Created {TNAME} (id={TID}).")

    playoff_rank_by_name = {n: i + 1 for i, n in enumerate(PLAYOFF_RANKS)}

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for eind_idx, (name_nl, pts, res) in enumerate(EINDKLASSEMENT, start=1):
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))

        final_rank = playoff_rank_by_name.get(name_nl, eind_idx)

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

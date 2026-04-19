"""Import BCLC 2024 Eindklassement + top-16 playoff ranking.

96 participants; top 16 have blank Pnt/Res (played playoffs),
17-96 have Pnt/Res.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 30
TNAME = "BCLC 2024"
TTYPE = "BCLC"
TYEAR = 2024

NAME_OVERRIDES = {
    "Raf Michels": "Raf Michiels",
    "Matias Vereecke": "Mattias Vereecke",
    "John Vanhees": "John Van Hees",
    "Tim Ongena": "Tim Onghena",
    "Karolien Thys": "Karolien Thijs",
    "Johan Van der Wal": "Johan Van Der Wal",
    "Ellen Hacquaert": "Ellen Hackaert",
    "Bjorn Petit": "Bj\u00f6rn Petit",
}

# Top-16 entries (Pnt/Res blank in source).
TOP16 = [
    "Harlinde Dewulf",
    "Niels Ongena",
    "Kenny Forrez",
    "Nico Wellemans",
    "Wannes Vansina",
    "Jochen Keymeulen",
    "Christine Coulon",
    "Maud Quaniers",
    "Lorenzo Van Herrewege",
    "Bart Dejaegher",
    "Kristof Kesteloot",
    "Fabian Mouton",
    "Jeroen Smolders",
    "Tania Vanderaerden",
    "Raf Mesotten",
    "Nicolas Victor",
]

# Eindklassement ranks 17..96: (name_nl, points, resistance)
TAIL = [
    ("Andres Hennebel",           19,   1.450496),
    ("Kristof Hennebel",          18,   1.424905),
    ("Els Smekens",               18,   1.41047),
    ("Johan Nuyts",               18,   1.351249),
    ("Klaas Govaerts",            17.5, 1.270225),
    ("Raf Michels",               17,   1.39146),     # -> Raf Michiels
    ("Florian Eloot",             17,   1.378664),
    ("Joanne Mackay",             17,   1.342232),
    ("Agnetha Van Herrewege",     16.5, 1.23359),
    ("Michiel Beerden",           16,   1.333236),
    ("Pieter De Vuyst",           16,   1.321531),
    ("Joachim Deschuytter",       16,   1.292191),
    ("Stijn Van der Stricht",     16,   1.287677),
    ("Nico Verlinden",            16,   1.281829),
    ("Elise Robberechts",         16,   1.208575),
    ("Hans Vanden Bosch",         15,   1.35417),
    ("Marissa Sorgeloos",         15,   1.318625),
    ("Christophe Heylen",         15,   1.295938),
    ("Kurt Verlinden",            15,   1.248817),
    ("Sylvia Foerier",            15,   1.182401),
    ("Andry Caluw\u00e9",         14,   1.465574),
    ("Karl Verheyden",            14,   1.33174),
    ("Christian Iguacel",         14,   1.287719),
    ("Matias Vereecke",           14,   1.28064),     # -> Mattias Vereecke
    ("Evelien Deneckere",         14,   1.274473),
    ("Joren De Ridder",           14,   1.274266),
    ("Bernard Smeets",            14,   1.257377),
    ("Sophie Peeters",            14,   1.250135),
    ("Lieselotte De Vuyst",       14,   1.235909),
    ("Stephen Fleerackers",       14,   1.18829),
    ("Maarten Van Camp",          14,   1.172721),
    ("Koen Claeys",               13,   1.384871),
    ("Vital Pluymers",            13,   1.273013),
    ("Philippe Kenens",           13,   1.226918),
    ("Ides Brabants",             13,   1.222743),
    ("Jolien De Schaepmeester",   13,   1.200861),
    ("Walther Mackay",            13,   1.194293),
    ("Wolf Nuyts",                13,   1.067224),
    ("Nicky Hendrickx",           12.5, 1.240015),
    ("Jasper Ongena",             12.5, 1.225195),
    ("An Delhaye",                12.5, 1.214048),
    ("Jelle Cuyt",                12.5, 1.195637),
    ("Simon Lecluse",             12,   1.303749),
    ("Karel Demeersseman",        12,   1.253594),
    ("Dave Segers",               12,   1.222899),
    ("Geert Wouters",             12,   1.217795),
    ("Johanna Dumon",             12,   1.202063),
    ("John Vanhees",              11,   1.181524),    # -> John Van Hees
    ("Anne-Sophie Troquet",       11,   1.160976),
    ("Bernard Peeters",           11,   1.106792),
    ("Borre Ongena",              11,   1.259196),
    ("Dag Kinne",                 11,   1.197977),
    ("Frederick Van Hulle",       11,   1.184001),
    ("Nicolas Rousseaux",         11,   1.166281),
    ("Lode Verlinden",            11,   1.165156),
    ("Ron Jongeneel",             11,   1.156966),
    ("Thomas Mackay",             11,   1.14701),
    ("Kim Diependaele",           11,   1.115356),
    ("Thomas Declerck",           11,   0.771605),
    ("Karolien Thys",             10.5, 1.087251),    # -> Karolien Thijs
    ("Frederick Goossens",        10,   1.189662),
    ("Tim Ongena",                10,   1.160559),    # -> Tim Onghena
    ("Hilde Walschaerts",         10,   1.154206),
    ("Marc Ongena",               10,   1.122929),
    ("Arnaud De Vuyst",           10,   0.921409),
    ("Gerrit Broeders",            9,   1.159784),
    ("Mieke Suenens",              9,   1.103267),
    ("Fien Wellemans",             9,   0.979271),
    ("Tom De Smedt",               9,   0.915577),
    ("Michiel Meire",              8,   0.822546),
    ("An Mees",                    8.5, 1.028455),
    ("Johan Van der Wal",          8,   1.167124),    # -> Johan Van Der Wal
    ("Brecht Volders",             8,   1.110112),
    ("Ellen Hacquaert",            8,   0.974898),    # -> Ellen Hackaert
    ("Marc Peeters",               8,   0.936885),
    ("Katrien Dellaert",           8,   0.892582),
    ("Tessa Van der Borght",       8,   0.711566),
    ("Bjorn Petit",                7,   0.928617),    # -> Björn Petit
    ("Dimitri Aertgeerts",         7,   0.684755),
    ("Michiel Demeersseman",       6.5, 0.96798),
]

# Top 16 playoff final ranks (1..16)
PLAYOFF_RANKS = [
    "Nico Wellemans",         # 1
    "Niels Ongena",           # 2
    "Nicolas Victor",         # 3
    "Jochen Keymeulen",       # 4
    "Fabian Mouton",          # 5
    "Bart Dejaegher",         # 6
    "Lorenzo Van Herrewege",  # 7
    "Kenny Forrez",           # 8
    "Raf Mesotten",           # 9
    "Maud Quaniers",          # 10
    "Jeroen Smolders",        # 11
    "Kristof Kesteloot",      # 12
    "Christine Coulon",       # 13
    "Harlinde Dewulf",        # 14
    "Wannes Vansina",         # 15
    "Tania Vanderaerden",     # 16
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
        [TID, TNAME, TTYPE, TYEAR, "2024", "2024-01-01",
         "Belgian Live Championship 2024 - 96 participants; top 16 played playoff "
         "(final rank 1-16), 17-96 from Eindklassement (blank Pnt/Res for top-16)."],
    )
    print(f"Created {TNAME} (id={TID}).")

    playoff_rank_by_name = {n: i + 1 for i, n in enumerate(PLAYOFF_RANKS)}

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []

    # Top 16 (Eindklassement ranks 1-16) — override with playoff final rank.
    for eind_rank, name_nl in enumerate(TOP16, start=1):
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))
        final_rank = playoff_rank_by_name[name_nl]
        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, resistance)
            VALUES (?, ?, ?, ?, NULL, NULL)
            """,
            [next_tp_id, TID, pid, final_rank],
        )
        next_tp_id += 1

    # Ranks 17..96
    for offset, (name_nl, pts, res) in enumerate(TAIL):
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))
        final_rank = 17 + offset
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

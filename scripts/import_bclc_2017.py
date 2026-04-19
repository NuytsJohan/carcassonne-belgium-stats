"""Import BCLC 2017 final ranking.

Source: BK Carcassonne 2017 (2).pdf (97 participants).
Top 8 played playoffs (Pnt/Res blank in Eindklassement). No playoff bracket in source.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 25
TNAME = "BCLC 2017"
TTYPE = "BCLC"
TYEAR = 2017

NAME_OVERRIDES = {
    "Gary Forrez": "Garry Forrez",
    "Matias Vereecke": "Mattias Vereecke",
    "Guy Cornelis": "Guy Comelis",
    "Raf Michels": "Raf Michiels",
    "Jaak Baes": "Jacques Baes",
    "Norbert Matteeuws": "Norbert Matheeuws",
    "Marie-Josee Duchateau": "Josee Duchateau",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Johan Nuyts",            None, None),
    (2,  "Wolf Nuyts",             None, None),
    (3,  "Nico Wellemans",         None, None),
    (4,  "Karl Verheyden",         None, None),
    (5,  "Simon Janssens",         None, None),
    (6,  "Gary Forrez",            None, None),   # -> Garry Forrez
    (7,  "Levi Hoste",             None, None),
    (8,  "Tim Afschrift",          None, None),

    (9,  "Christophe Nechelput",   24, 0.2812),
    (10, "Wannes Vansina",         22, 0.2791),
    (11, "Willem Vansina",         22, 0.2772),
    (12, "Charlotte Van Gulik",    22, 0.2684),
    (13, "Lorenzo Van Herrewege",  21, 0.2809),
    (14, "Xavier Suykens",         21, 0.2677),
    (15, "Bruno De Lat",           21, 0.2629),
    (16, "Nathan Declerck",        21, 0.2608),
    (17, "Raf Mesotten",           21, 0.2540),
    (18, "Iris Coosemans",         20, 0.2685),
    (19, "Tom Van Herrewege",      20, 0.2654),
    (20, "Ineke Hoolants",         20, 0.2613),
    (21, "Steve Maris",            20, 0.2579),
    (22, "Jelle Van Goethem",      20, 0.2518),
    (23, "Olivier Claessens",      19, 0.2693),
    (24, "Kenneth Lingier",        19, 0.2678),
    (25, "Guy Cornelis",           19, 0.2666),   # -> Guy Comelis
    (26, "Jeroen Janssens",        19, 0.2630),
    (27, "Gert Geebelen",          19, 0.2609),
    (28, "Kenny Forrez",           19, 0.2606),
    (29, "Nicolas Victor",         19, 0.2583),
    (30, "Renaud Godfraind",       19, 0.2582),
    (31, "Jeffry Mostrey",         19, 0.2571),
    (32, "Tom Noppe",              18, 0.2694),
    (33, "Jan Heremans",           18, 0.2682),
    (34, "Hans Baes",              18, 0.2632),
    (35, "Youri Blomme",           18, 0.2560),
    (36, "Nele De Pooter",         18, 0.2484),
    (37, "Tom Ribbens",            18, 0.2480),
    (38, "Patrick Stuer",          17, 0.2662),
    (39, "David September",        17, 0.2636),
    (40, "Vital Pluymers",         17, 0.2622),
    (41, "Johan Leflot",           17, 0.2620),
    (42, "David Hantson",          17, 0.2600),
    (43, "Peter Nuyts",            17, 0.2567),
    (44, "Sam Stevens",            17, 0.2558),
    (45, "Matias Vereecke",        17, 0.2553),   # -> Mattias Vereecke
    (46, "Wietze Lievers",         17, 0.2546),
    (47, "Niels Ongena",           17, 0.2486),
    (48, "Peter Leflot",           17, 0.2473),
    (49, "Nicky Hendrickx",        17, 0.2384),
    (50, "Griet Follet",           16, 0.2673),
    (51, "Dave Segers",            16, 0.2545),
    (52, "Bob Eeltink",            16, 0.2534),
    (53, "Bertrand Hallyn",        16, 0.2503),
    (54, "Willem Van Bogaert",     16, 0.2460),
    (55, "Jasper Desmadryl",       16, 0.2385),
    (56, "Marie-Josee Duchateau",  16, 0.2349),   # -> Josee Duchateau
    (57, "Maxime Paviltch",        15, 0.2658),
    (58, "Mieke Lemarcq",          15, 0.2602),
    (59, "Steven Hoste",           15, 0.2584),
    (60, "Benjamin Symons",        15, 0.2553),
    (61, "Jana Tielens",           15, 0.2527),
    (62, "Tania Vanderaerden",     15, 0.2495),
    (63, "Toon Van Herrewege",     15, 0.2473),
    (64, "Benjamin Meeus",         15, 0.2460),
    (65, "Joren De Ridder",        15, 0.2438),
    (66, "Els Smekens",            15, 0.2417),
    (67, "Jonas Vereecken",        15, 0.2417),
    (68, "Bart Dejaegher",         15, 0.2304),
    (69, "Israel Benain Peral",    14, 0.2535),
    (70, "Anneleen Van Nuffel",    14, 0.2461),
    (71, "Bram Schoors",           14, 0.2433),
    (72, "Thomas Van Belle",       14, 0.2343),
    (73, "Jaak Baes",              14, 0.2314),   # -> Jacques Baes
    (74, "Grim Ongena",            14, 0.2290),
    (75, "Keaf Buytaert",          14, 0.2262),
    (76, "Keano Heremans",         13, 0.2991),
    (77, "Ken Brands",             13, 0.2696),
    (78, "Manja Vandenhoven",      13, 0.2407),
    (79, "Pascal Wellemans",       13, 0.2365),
    (80, "Peter Rombouts",         13, 0.2295),
    (81, "Agnetha Van Herrewege",  13, 0.2285),
    (82, "Raf Michels",            12, 0.2406),   # -> Raf Michiels
    (83, "Lina Van Hecke",         12, 0.2374),
    (84, "Lieve Peirtsegaele",     12, 0.2317),
    (85, "Floris Buytaert",        12, 0.2269),
    (86, "Bart Heremans",          12, 0.2248),
    (87, "Jiliam Theys",           12, 0.2169),
    (88, "Jelle Wevers",           11, 0.2455),
    (89, "Lorena Sanchez Felipe",  11, 0.2412),
    (90, "Davy Forrez",            11, 0.2304),
    (91, "Norbert Matteeuws",      11, 0.2276),   # -> Norbert Matheeuws
    (92, "Bart Drijkoningen",      11, 0.2273),
    (93, "Evy Pelegrin",           11, 0.2016),
    (94, "Johan Van Der Wal",      10, 0.2313),
    (95, "Ronny Theys",             7, 0.2430),
    (96, "Michiel Simons",          7, 0.2210),
    (97, "Eef Wouters",             0, 0.0000),
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
        [TID, TNAME, TTYPE, TYEAR, "2017", "2017-01-01",
         "Belgian Live Championship 2017 - Eindklassement (97 participants). "
         "Top 8 played playoffs; bracket not included in source PDF."],
    )
    print(f"Created {TNAME} (id={TID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, pts, res in PARTICIPANTS:
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

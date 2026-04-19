"""Import BCLC 2018 final ranking.

Source: BK Carcassonne 2018.pdf (92 participants).
Top 8 played playoffs (Pnt/Res blank). No playoff bracket in source.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 26
TNAME = "BCLC 2018"
TTYPE = "BCLC"
TYEAR = 2018

NAME_OVERRIDES = {
    "Raf Michels": "Raf Michiels",
    "Guy Cornelis": "Guy Comelis",
    "Juan Carlos Martin": "Juan Carlos Martin Severiano",
    "Manja Vandenhove": "Manja Vandenhoven",
    "Ellen Hacquaert": "Ellen Hackaert",
    "Matias Vereecke": "Mattias Vereecke",
    "Jaak Baes": "Jacques Baes",
    "Hans Gyd\u00e9": "Hans Gijd\u00e9",
    "Bruno Delat": "Bruno De Lat",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Joren De Ridder",        None, None),
    (2,  "Karl Verheyden",         None, None),
    (3,  "Tom Van Herrewege",      None, None),
    (4,  "John Van Hees",          None, None),
    (5,  "Jurgen Spreutels",       None, None),
    (6,  "Raf Michels",            None, None),   # -> Raf Michiels
    (7,  "Seijke Weeghmans",       None, None),
    (8,  "An Delhaye",             None, None),

    (9,  "Tom Noppe",              24, 0.2770),
    (10, "Nele De Pooter",         22, 0.2909),
    (11, "Guy Cornelis",           22, 0.2785),   # -> Guy Comelis
    (12, "Juan Carlos Martin",     21, 0.2749),   # -> Juan Carlos Martin Severiano
    (13, "Kenny Forrez",           21, 0.2740),
    (14, "Godelieve Peirtsegaele", 21, 0.2707),
    (15, "Hans Baes",              21, 0.2672),
    (16, "Manja Vandenhove",       21, 0.2621),   # -> Manja Vandenhoven
    (17, "Nicolas Victor",         20, 0.2684),
    (18, "Nathan Declerck",        20, 0.2652),
    (19, "Agnetha Van Herrewege",  20, 0.2514),
    (20, "Johan Nuyts",            19, 0.2786),
    (21, "Tine Plessers",          19, 0.2690),
    (22, "Nico Wellemans",         19, 0.2604),
    (23, "Mats Elsen",             19, 0.2600),
    (24, "Tania Vanderaerden",     19, 0.2593),
    (25, "Roland Persoon",         19, 0.2496),
    (26, "Jasper Desmadryl",       18, 0.2783),
    (27, "Youri Blomme",           18, 0.2737),
    (28, "Kim Peeters",            18, 0.2657),
    (29, "Ellen Hacquaert",        18, 0.2618),   # -> Ellen Hackaert
    (30, "Dave Segers",            18, 0.2616),
    (31, "Raf Mesotten",           18, 0.2597),
    (32, "Mieke Lemarcq",          18, 0.2587),
    (33, "Wannes Vansina",         18, 0.2542),
    (34, "Vital Pluymers",         18, 0.2534),
    (35, "Joren Peeters",          18, 0.2527),
    (36, "Tim Van den Bosche",     18, 0.2459),
    (37, "Jeroen Plessers",        17, 0.2663),
    (38, "Rita De Vos",            17, 0.2625),
    (39, "Lorenzo Van Herrewege",  17, 0.2538),
    (40, "Sam Stevens",            17, 0.2533),
    (41, "Jeffry Mostrey",         17, 0.2520),
    (42, "Gert Geebelen",          17, 0.2498),
    (43, "Peter Nuyts",            17, 0.2493),
    (44, "Bart Dejaegher",         17, 0.2453),
    (45, "Grim Ongena",            17, 0.2414),
    (46, "Thibault Heylen",        17, 0.2403),
    (47, "Jelle Van Goethem",      17, 0.2323),
    (48, "Johan Van Der Wal",      16, 0.2611),
    (49, "David Hantson",          16, 0.2608),
    (50, "Xavier Suykens",         16, 0.2576),
    (51, "Els Smekens",            16, 0.2508),
    (52, "Pieter Verschraegen",    16, 0.2465),
    (53, "Willem Van Bogaert",     16, 0.2403),
    (54, "Josee Duchateau",        16, 0.2312),
    (55, "Matias Vereecke",        16, 0.2279),   # -> Mattias Vereecke
    (56, "Teun Swerts",            15, 0.2496),
    (57, "Bertrand Hallyn",        15, 0.2479),
    (58, "Tom Ribbens",            15, 0.2452),
    (59, "Garry Forrez",           15, 0.2423),
    (60, "Celien Neven",           15, 0.2413),
    (61, "Floris Buytaert",        15, 0.2340),
    (62, "Inneke Burssens",        15, 0.2339),
    (63, "Benjamin Symons",        15, 0.2313),
    (64, "Keaf Buytaert",          15, 0.2299),
    (65, "Marc Peeters",           14, 0.2528),
    (66, "Simon Janssens",         14, 0.2472),
    (67, "Toon Van Herrewege",     14, 0.2432),
    (68, "Hans Gyd\u00e9",         14, 0.2398),   # -> Hans Gijdé
    (69, "Bruno Delat",            14, 0.2354),   # -> Bruno De Lat
    (70, "Steve Maris",            14, 0.2352),
    (71, "Arnaud De Vuyst",        14, 0.2331),
    (72, "Niels Ongena",           13, 0.2487),
    (73, "Katrien Dellaert",       13, 0.2478),
    (74, "Daisy Maes",             13, 0.2467),
    (75, "Pascal Wellemans",       13, 0.2441),
    (76, "Christophe Nechelput",   13, 0.2405),
    (77, "Pieterjan Van Laethem",  13, 0.2358),
    (78, "Olivier Claessens",      13, 0.2351),
    (79, "Niels Engelen",          13, 0.2342),
    (80, "Jonas Vereecken",        13, 0.2216),
    (81, "Anneleen Van Nuffel",    12, 0.2542),
    (82, "Jaak Baes",              12, 0.2445),   # -> Jacques Baes
    (83, "Gjalt Vanhouwaert",      12, 0.2188),
    (84, "Yana Goossens",          12, 0.2092),
    (85, "Michiel Beerden",        12, 0.1863),
    (86, "Renaud Godfraind",       11, 0.2726),
    (87, "Patrick Stuer",          11, 0.2356),
    (88, "Steven Hoste",           11, 0.2297),
    (89, "Karolien Thijs",         11, 0.2258),
    (90, "Maarten Van Camp",       11, 0.2204),
    (91, "Nicky Hendriks",         10, 0.2146),
    (92, "Greet Mathijs",           7, 0.2051),
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
        [TID, TNAME, TTYPE, TYEAR, "2018", "2018-01-01",
         "Belgian Live Championship 2018 - Eindklassement (92 participants). "
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

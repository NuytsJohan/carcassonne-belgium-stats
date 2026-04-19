"""Import BCLC 2019 final ranking.

Source: BK Carcassonne 2019.pdf (72 participants).
Top 8 played playoffs (Pnt/Res blank). No playoff bracket in source.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 27
TNAME = "BCLC 2019"
TTYPE = "BCLC"
TYEAR = 2019

NAME_OVERRIDES = {
    "Raf Michels": "Raf Michiels",
    "Juan Carlos Martin": "Juan Carlos Martin Severiano",
    "John Vanhees": "John Van Hees",
    "Guy Cornelis": "Guy Comelis",
    "Nathan Declerk": "Nathan Declerck",
    "Renaud Godfreind": "Renaud Godfraind",
    "Celin Neven": "Celien Neven",
    "Ann Delhaye": "An Delhaye",
    "Jeffry Mostry": "Jeffry Mostrey",
    "Jaak Baes": "Jacques Baes",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Raf Michels",            None, None),   # -> Raf Michiels
    (2,  "Lieve Peirtsegaele",     None, None),
    (3,  "Tom Noppe",              None, None),
    (4,  "Johan Van Der Wal",      None, None),
    (5,  "Jeroen Janssens",        None, None),
    (6,  "Karl Verheyden",         None, None),
    (7,  "Michiel Beerden",        None, None),
    (8,  "Joren De Ridder",        None, None),

    (9,  "Joke Stroobants",             23, 0.2709),
    (10, "Annemie Van Der Smissen",     22, 0.2769),
    (11, "Vital Pluymers",              22, 0.2710),
    (12, "Juan Carlos Martin",          21, 0.2627),   # -> Juan Carlos Martin Severiano
    (13, "John Vanhees",                21, 0.2512),   # -> John Van Hees
    (14, "Guy Cornelis",                20, 0.2667),   # -> Guy Comelis
    (15, "Lorenzo Van Herrewege",       20, 0.2653),
    (16, "Davy Forrez",                 20, 0.2633),
    (17, "Jurgen Spreutels",            20, 0.2598),
    (18, "Hans Baes",                   20, 0.2588),
    (19, "Nathan Declerk",              19, 0.2627),   # -> Nathan Declerck
    (20, "Niels Ongena",                19, 0.2617),
    (21, "Renaud Godfreind",            19, 0.2536),   # -> Renaud Godfraind
    (22, "Tania Vanderaerden",          19, 0.2518),
    (23, "Nicky Hendriks",              18, 0.2647),
    (24, "Mattias Vereecke",            18, 0.2645),
    (25, "Youri Blomme",                18, 0.2639),
    (26, "Johan Nuyts",                 18, 0.2586),
    (27, "Simon Janssens",              18, 0.2538),
    (28, "Arnaud De Vuyst",             18, 0.2532),
    (29, "Dominic R\u00e9b\u00e9rez",   18, 0.2519),
    (30, "Celin Neven",                 18, 0.2496),   # -> Celien Neven
    (31, "Manja Vandenhoven",           17, 0.2595),
    (32, "Floris Buytaert",             17, 0.2587),
    (33, "Kenny Forrez",                17, 0.2556),
    (34, "Grim Ongena",                 17, 0.2489),
    (35, "Tom Van Herrewege",           17, 0.2487),
    (36, "Bertrand Hallyn",             17, 0.2482),
    (37, "Nico Wellemans",              16, 0.2583),
    (38, "Florian Eloot",               16, 0.2542),
    (39, "Cedric Dezitter",             16, 0.2519),
    (40, "Raf Mesotten",                16, 0.2492),
    (41, "Ann Delhaye",                 16, 0.2484),   # -> An Delhaye
    (42, "Benjamin Symons",             16, 0.2335),
    (43, "Robin Lenaerts",              16, 0.2331),
    (44, "Tom Segers",                  16, 0.2270),
    (45, "Pascal Wellemans",            15, 0.2543),
    (46, "Patrick De Wilde",            15, 0.2508),
    (47, "Keaf Buytaert",               15, 0.2494),
    (48, "Nele De Pooter",              15, 0.2471),
    (49, "Peter Leflot",                15, 0.2451),
    (50, "Seijke Weeghmans",            15, 0.2404),
    (51, "Elise Vandenabeele",          15, 0.2379),
    (52, "Kristof Roose",               15, 0.2245),
    (53, "Jef De Ridder",               14, 0.2653),
    (54, "Els Smekens",                 14, 0.2511),
    (55, "Tom Ribbens",                 14, 0.2480),
    (56, "Josee Duchateau",             14, 0.2426),
    (57, "Brenda Stappers",             14, 0.2419),
    (58, "Tim Onghena",                 14, 0.2289),
    (59, "Agnetha Van Herrewege",       13, 0.2358),
    (60, "Steve Maris",                 13, 0.2334),
    (61, "Jaak Baes",                   13, 0.2286),   # -> Jacques Baes
    (62, "Karolien Thijs",              13, 0.2235),
    (63, "Mats Elsen",                  12, 0.2393),
    (64, "Xavier Suykens",              12, 0.2270),
    (65, "Lasse R\u00e9b\u00e9rez",     12, 0.2262),
    (66, "Marc Ongena",                 12, 0.2246),
    (67, "Cindy Nelissen",              11, 0.2366),
    (68, "Jelina Luyckx",               11, 0.2267),
    (69, "Andy Baron",                  10, 0.2314),
    (70, "Jeffry Mostry",               10, 0.2227),   # -> Jeffry Mostrey
    (71, "Borre Ongena",                10, 0.2137),
    (72, "Ineke Burssens",               9, 0.2075),
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
        [TID, TNAME, TTYPE, TYEAR, "2019", "2019-01-01",
         "Belgian Live Championship 2019 - Eindklassement (72 participants). "
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

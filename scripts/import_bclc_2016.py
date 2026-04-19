"""Import BCLC 2016 final ranking.

Source: BK Carcassonne 2016 - Eindklassement.pdf (97 participants).
Top 8 played playoffs and have no Pnt/Res. No playoff bracket included in PDF.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 24
TNAME = "BCLC 2016"
TTYPE = "BCLC"
TYEAR = 2016

NAME_OVERRIDES = {
    # Match existing DB spellings (including earlier OCR quirks):
    "Guy Cornelis": "Guy Comelis",     # existing 2015 import used OCR form 'Comelis'
    "Raf Michels": "Raf Michiels",     # existing 2015 spelling
    "Simon Janssen": "Simon Janssens",
    "Jaak Baes": "Jacques Baes",
    "Jaak Evens": "Jacques Evens",
    "Robbe Vervake": "Robbe Vervaeke",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Wannes Vansina",        None, None),
    (2,  "Marc Peeters",          None, None),
    (3,  "Renaud Godfraind",      None, None),
    (4,  "Dave Segers",           None, None),
    (5,  "Karl Verheyden",        None, None),
    (6,  "Nicolas Victor",        None, None),
    (7,  "Hans Baes",             None, None),
    (8,  "Rita De Vos",           None, None),

    (9,  "Johan Van Der Wal",     25, 0.2997),
    (10, "Willem Van Bogaert",    23, 0.2796),
    (11, "Youri Blomme",          22, 0.2777),
    (12, "Roland Persoon",        22, 0.2733),
    (13, "David September",       22, 0.2724),
    (14, "Joren De Ridder",       21, 0.2779),
    (15, "Agnetha Van Herrewege", 21, 0.2662),
    (16, "Garry Forrez",          20, 0.2870),
    (17, "Keaf Buytaert",         20, 0.2702),
    (18, "Gert Geebelen",         20, 0.2672),
    (19, "Christophe Nechelput",  20, 0.2656),
    (20, "Bart Dejaegher",        19, 0.2851),
    (21, "Willem Vansina",        19, 0.2776),
    (22, "Kim Bassens",           19, 0.2740),
    (23, "Pascal Wellemans",      19, 0.2700),
    (24, "Els Smekens",           19, 0.2680),
    (25, "Tom Ribbens",           19, 0.2672),
    (26, "Lorenzo Van Herrewege", 19, 0.2653),
    (27, "Johan Nuyts",           19, 0.2630),
    (28, "Guy Cornelis",          19, 0.2618),   # -> Guy Comelis (existing)
    (29, "Lieve Peirtsegaele",    19, 0.2568),
    (30, "Raf Michels",           19, 0.2513),   # -> Raf Michiels (existing)
    (31, "Peter Rombouts",        18, 0.2730),
    (32, "Hans Gijd\u00e9",       18, 0.2698),
    (33, "Bram Ackermans",        18, 0.2657),
    (34, "Micha\u00ebl Maley",    18, 0.2655),
    (35, "Sander Willems",        18, 0.2611),
    (36, "Steve Hosten",          18, 0.2605),
    (37, "Benjamin Symons",       18, 0.2586),
    (38, "Tania Vanderaerden",    18, 0.2424),
    (39, "Anita Feyer",           17, 0.2720),
    (40, "Raf Mesotten",          17, 0.2681),
    (41, "Bj\u00f6rn Petit",      17, 0.2639),
    (42, "Ineke Hoolants",        17, 0.2622),
    (43, "Bertrand Hallyn",       17, 0.2606),
    (44, "Tom Van Herrewege",     17, 0.2571),
    (45, "Nathalie Garnier",      17, 0.2568),
    (46, "Tom Segers",            17, 0.2478),
    (47, "Nicky Hendriks",        17, 0.2459),
    (48, "Charlotte Van Gulik",   16, 0.2836),
    (49, "Iris Renard",           16, 0.2725),
    (50, "Simon Janssen",         16, 0.2713),   # -> Simon Janssens
    (51, "Tom Vandermeersch",     16, 0.2708),
    (52, "Elien Vangheluwe",      16, 0.2659),
    (53, "Jos Van Landeghem",     16, 0.2623),
    (54, "Bob Eeltink",           16, 0.2506),
    (55, "Ben Verschooris",       16, 0.2499),
    (56, "Manon Zonder",          16, 0.2429),
    (57, "Gunther Van Geetsom",   16, 0.2386),
    (58, "Lotte Koning",          16, 0.2155),
    (59, "Jonas Vereecken",       15, 0.2756),
    (60, "Mathias Vereecken",     15, 0.2654),
    (61, "Jaak Baes",             15, 0.2648),   # -> Jacques Baes
    (62, "Tim Afschrift",         15, 0.2624),
    (63, "Arnaud De Vuyst",       15, 0.2536),
    (64, "Kenny Forrez",          15, 0.2475),
    (65, "Ronny Thijs",           15, 0.2463),
    (66, "Nathan Declerck",       15, 0.2421),
    (67, "Anneleen Van Nuffel",   15, 0.2393),
    (68, "Tamara Gelaude",        15, 0.2360),
    (69, "Leen Slegt",            15, 0.2241),
    (70, "Wolf Nuyts",            14, 0.2628),
    (71, "Mieke Lemarcq",         14, 0.2498),
    (72, "Tom Dewitte",           14, 0.2491),
    (73, "Levi Hoste",            14, 0.2479),
    (74, "Josee Duchateau",       14, 0.2478),
    (75, "Jaak Evens",            14, 0.2450),   # -> Jacques Evens
    (76, "Peter Leflot",          14, 0.2422),
    (77, "Katrien Dellaert",      14, 0.2348),
    (78, "Mitch Reniers",         14, 0.2336),
    (79, "Robbe Vervake",         13, 0.2561),   # -> Robbe Vervaeke
    (80, "Laurent Genin",         13, 0.2532),
    (81, "Nico Wellemans",        13, 0.2423),
    (82, "Lieselotte De Vuyst",   13, 0.2372),
    (83, "Maud Victorian",        13, 0.2341),
    (84, "Kenneth Lingier",       12, 0.2458),
    (85, "Sam Stevens",           12, 0.2426),
    (86, "Peter Nuyts",           12, 0.2408),
    (87, "Jurgen Spreutels",      12, 0.2131),
    (88, "Stef Louwyck",          11, 0.2537),
    (89, "Floris Buytaert",       11, 0.2508),
    (90, "Norbert Matheeuws",     11, 0.2477),
    (91, "Jonas Aerts",           11, 0.2454),
    (92, "Olivier Claessens",     11, 0.2019),
    (93, "Lina Van Hecke",        10, 0.2105),
    (94, "Ellen Van Akelyen",      9, 0.2419),
    (95, "David Hantson",          9, 0.1754),
    (96, "Bart Heremans",          7, 0.2337),
    (97, "Patrick Stuer",          7, 0.1925),
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
        [TID, TNAME, TTYPE, TYEAR, "2016", "2016-01-01",
         "Belgian Live Championship 2016 - Eindklassement (97 participants). "
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

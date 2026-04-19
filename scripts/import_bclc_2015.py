"""Import BCLC 2015 final ranking + top-8 playoff bracket.

Source: screenshot of 2015 Eindklassement (78 participants, top 8 playoffs).
Playoff bracket structure inferred from the same {1v6, 4v8, 2v7, 3v5} pattern
used in 2013/2014 BCLC editions. Scores of playoff matches not available.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 23
TNAME = "BCLC 2015"
TTYPE = "BCLC"
TYEAR = 2015

NAME_OVERRIDES = {
    "Hans Gyd\u00e9": "Hans Gijd\u00e9",     # OCR typo of ij->y
    "Garry Forez": "Garry Forrez",
    "Tijs Raman": "Thijs Raman",
    "Bjorn Petit": "Bj\u00f6rn Petit",
    "Simon Janssen": "Simon Janssens",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Joren De Ridder",       None, None),
    (2,  "Kenny Forrez",          None, None),
    (3,  "Nele De Pooter",        None, None),
    (4,  "Marc Peeters",          None, None),
    (5,  "Karl Verheyden",        None, None),
    (6,  "Agnetha Van Herrewege", None, None),
    (7,  "Nico Wellemans",        None, None),
    (8,  "Joris Van Steenberghe", None, None),

    (9,  "Hans Gyd\u00e9",        23, 0.2770),   # -> Hans Gijdé
    (10, "Garry Forez",           22, 0.2710),   # -> Garry Forrez
    (11, "Tijs Raman",            21, 0.2908),   # -> Thijs Raman
    (12, "Hans Baes",             21, 0.2836),
    (13, "Willem Van Bogaert",    21, 0.2651),
    (14, "Lorenzo Van Herrewege", 21, 0.2635),
    (15, "Siebrecht Descamps",    20, 0.2741),
    (16, "Johan Nuyts",           20, 0.2738),
    (17, "Willem Vansina",        20, 0.2697),
    (18, "Peter Nuyts",           20, 0.2669),
    (19, "Lieve Peirtsegaele",    19, 0.2851),
    (20, "Wannes Vansina",        19, 0.2754),
    (21, "Koenraad Stevens",      19, 0.2776),
    (22, "Pascal Wellemans",      19, 0.2560),
    (23, "Tim Afschrift",         19, 0.2537),
    (24, "Patrick Stuer",         19, 0.2522),
    (25, "Bruno De Lat",          18, 0.2751),
    (26, "Jonas Aerts",           18, 0.2751),
    (27, "Bram Ackermans",        18, 0.2626),
    (28, "Steve Maris",           18, 0.2561),
    (29, "Benjamin Symons",       18, 0.2465),
    (30, "Christophe Nechelput",  18, 0.2398),
    (31, "Micha\u00ebl Maley",    17, 0.2764),
    (32, "Arnaud De Vuyst",       17, 0.2708),
    (33, "Steven Vervaet",        17, 0.2651),
    (34, "Jonas Vereecken",       17, 0.2643),
    (35, "Bjorn Petit",           17, 0.2582),   # -> Björn Petit
    (36, "Bob Eeltink",           17, 0.2508),
    (37, "Stef Louwyck",          17, 0.2456),
    (38, "Nathan Declerck",       17, 0.2445),
    (39, "Simon Janssen",         17, 0.2407),   # -> Simon Janssens
    (40, "Raf Michiels",          16, 0.2630),
    (41, "Rita De Vos",           16, 0.2615),
    (42, "Dominick Swinnen",      16, 0.2609),
    (43, "Olivier Claessens",     16, 0.2520),
    (44, "Juan Carlos Martin Severiano", 16, 0.2512),
    (45, "Bertrand Hallyn",       16, 0.2478),
    (46, "Franky Cnudde",         16, 0.2478),
    (47, "Dirk Rattat",           16, 0.2448),
    (48, "Jo Pirard",             16, 0.2337),
    (49, "Charlotte Van Gulik",   15, 0.2806),
    (50, "Johan Leflot",          15, 0.2664),
    (51, "Shana Verschueren",     15, 0.2607),
    (52, "Bart Dejaegher",        15, 0.2510),
    (53, "Joost Vangoidsenhoven", 15, 0.2497),
    (54, "Melissa Hendrickx",     15, 0.2481),
    (55, "Melissa Verschueren",   15, 0.2464),
    (56, "Pieter Verschraegen",   15, 0.2455),
    (57, "Florian Cnudde",        15, 0.2434),
    (58, "Johan Van Der Wal",     15, 0.2338),
    (59, "Guy Comelis",           15, 0.2306),
    (60, "Wim Tuijns",            14, 0.3091),
    (61, "Gert Geebelen",         14, 0.2526),
    (62, "Jan Blankaert",         14, 0.2408),
    (63, "Anneleen Van Nuffel",   14, 0.2403),
    (64, "Gunther Van Geetsom",   14, 0.2373),
    (65, "Maud Victorian",        14, 0.2316),
    (66, "Pauline Denivel",       13, 0.2947),
    (67, "Dave Segers",           13, 0.2710),
    (68, "Wolf Nuyts",            13, 0.2474),
    (69, "Norbert Afschrift",     13, 0.2448),
    (70, "Katrien Dellaert",      13, 0.2422),
    (71, "Roland Persoon",        13, 0.2375),
    (72, "Nick Leentjes",         13, 0.2315),
    (73, "Bob Schroyen",          13, 0.2614),
    (74, "Laurent Genin",         13, 0.2486),
    (75, "Kevin Leflot",          12, 0.2393),
    (76, "David Hantson",         12, 0.2230),
    (77, "Mattias Vereecke",      10, 0.2692),
    (78, "Rein Daneels",          10, 0.2034),
]

# Playoff bracket (seeds = top-8 Eindklassement order).
# QF pattern {1v6, 4v8, 2v7, 3v5}; winners = higher seed.
PLAYOFFS = [
    ("1/4 Finals",        1, "Joren De Ridder",       "Agnetha Van Herrewege", "1"),
    ("1/4 Finals",        2, "Marc Peeters",          "Joris Van Steenberghe", "1"),
    ("1/4 Finals",        3, "Kenny Forrez",          "Nico Wellemans",        "1"),
    ("1/4 Finals",        4, "Nele De Pooter",        "Karl Verheyden",        "1"),

    ("1/2 Finals",        1, "Joren De Ridder",       "Marc Peeters",          "1"),
    ("1/2 Finals",        2, "Kenny Forrez",          "Nele De Pooter",        "1"),

    ("Loser Bracket R1",  1, "Agnetha Van Herrewege", "Joris Van Steenberghe", "1"),
    ("Loser Bracket R1",  2, "Karl Verheyden",        "Nico Wellemans",        "1"),

    ("Final",     1, "Joren De Ridder",       "Kenny Forrez",          "1"),
    ("3rd Place", 1, "Nele De Pooter",        "Marc Peeters",          "1"),
    ("5th Place", 1, "Karl Verheyden",        "Agnetha Van Herrewege", "1"),
    ("7th Place", 1, "Nico Wellemans",        "Joris Van Steenberghe", "1"),
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
        [TID, TNAME, TTYPE, TYEAR, "2015", "2015-01-01",
         "Belgian Live Championship 2015 - Eindklassement + top-8 playoff bracket "
         "(scores not extracted)."],
    )
    print(f"Created {TNAME} (id={TID}).")

    pid_by_name: dict[str, int] = {}

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for rank, name_nl, pts, res in PARTICIPANTS:
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        pid_by_name[canonical] = pid
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

    # --- Playoff matches ---
    max_tm = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_matches").fetchone()[0]
    next_tm = max_tm + 1

    for stage, mnum, p1, p2, result in PLAYOFFS:
        a_pid = pid_by_name[p1]
        b_pid = pid_by_name[p2]
        table_no = mnum if stage == "1/4 Finals" else None

        gid = conn.execute(
            """
            INSERT INTO games (tournament_id, round, table_number, source, played_at)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
            """,
            [TID, 1, table_no, "manual", "2015-01-01"],
        ).fetchone()[0]

        a_rank = 1 if result == "1" else 2
        b_rank = 2 if result == "1" else 1
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, NULL, ?)",
            [gid, a_pid, a_rank],
        )
        conn.execute(
            "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, NULL, ?)",
            [gid, b_pid, b_rank],
        )
        conn.execute(
            """
            INSERT INTO tournament_matches
                (id, tournament_id, stage, match_number, player_1_id, player_2_id,
                 score_1, score_2, result, notes, game_id_1)
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, ?)
            """,
            [next_tm, TID, stage, mnum, a_pid, b_pid, result, gid],
        )
        next_tm += 1
        print(f"  [{stage} #{mnum}] {p1} def {p2}  game_id={gid}")

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    gcnt = conn.execute("SELECT COUNT(*) FROM games WHERE tournament_id = ?", [TID]).fetchone()[0]
    tmcnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    print(f"\nParticipants: {cnt}, games: {gcnt}, tournament_matches: {tmcnt}")

    conn.close()


if __name__ == "__main__":
    main()

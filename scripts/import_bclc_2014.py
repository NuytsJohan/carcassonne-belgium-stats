"""Import BCLC 2014 final ranking + top-8 playoff bracket.

Source: BK Carcassonne 2014.pdf
- Page 1-2 Eindklassement: 64 participants. Top 8 played a knockout playoff
  (rank only, no Pnt/Res).
- Page 3 Eindronde bracket: used to derive the 12 playoff matches.
  QF pairings follow the same pattern as 2013: {1v6, 4v8, 2v7, 3v5}.
  Winners of each QF go to Semi (winners' bracket); losers go to Loser R1.
  From final ranks we recover all 12 winner/loser decisions.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 22
TNAME = "BCLC 2014"
TTYPE = "BCLC"
TYEAR = 2014

NAME_OVERRIDES = {
    "Jaak Baes": "Jacques Baes",
    "Sylvia Fourrier": "Sylvia Foerier",
    "Ellen Hacquaert": "Ellen Hackaert",
    "Bjorn Petit": "Bj\u00f6rn Petit",
}

# (final_rank, name_nl, pts_or_None, res_or_None)
PARTICIPANTS = [
    (1,  "Joren Peeters",             None, None),
    (2,  "Hans Baes",                 None, None),
    (3,  "Johan Nuyts",               None, None),
    (4,  "Elien Vangheluwe",          None, None),
    (5,  "Willem Vansina",            None, None),
    (6,  "Jonas Vereecken",           None, None),
    (7,  "Simon Janssens",            None, None),
    (8,  "Pieter Verschraegen",       None, None),
    (9,  "Peter Nuyts",               22, 0.2834),
    (10, "Dieter Peeters",            22, 0.2787),
    (11, "Bruno De Lat",              22, 0.2750),
    (12, "Kenny Forrez",              21, 0.2685),
    (13, "Rita De Vos",               21, 0.2645),
    (14, "Johan Van Der Wal",         20, 0.2568),
    (15, "Hans Gijd\u00e9",           20, 0.2547),
    (16, "Nele De Pooter",            19, 0.2765),
    (17, "Steven Van den Bosch",      19, 0.2672),
    (18, "Wannes Vansina",            19, 0.2610),
    (19, "Sander Willems",            18, 0.2881),
    (20, "Mart Grommen",              18, 0.2725),
    (21, "Jurgen Spreutels",          18, 0.2657),
    (22, "Steven Vervaet",            18, 0.2633),
    (23, "Jaak Baes",                 18, 0.2522),   # -> Jacques Baes
    (24, "Bjorn Petit",               18, 0.2494),   # -> Björn Petit
    (25, "Benjamin Symons",           18, 0.2470),
    (26, "Garry Forrez",              17, 0.2680),
    (27, "Greet De Smet",             17, 0.2516),
    (28, "Tamara Gelaude",            17, 0.2509),
    (29, "Delphine Claeys",           17, 0.2494),
    (30, "Paulien Denivel",           17, 0.2468),
    (31, "Bart Gerard",               17, 0.2439),
    (32, "Karl Verheyden",            16, 0.2730),
    (33, "Katrien Dellaert",          16, 0.2611),
    (34, "Pascal Wellemans",          16, 0.2580),
    (35, "Nicolas Victor",            16, 0.2575),
    (36, "Dave Segers",               16, 0.2512),
    (37, "Willem Van Bogaert",        16, 0.2372),
    (38, "Wim Tuijns",                16, 0.2316),
    (39, "Sylvia Fourrier",           16, 0.2261),   # -> Sylvia Foerier
    (40, "Bart Descamps",             16, 0.2226),
    (41, "Thijs Raman",               15, 0.2463),
    (42, "Kevin Willems",             15, 0.2438),
    (43, "Ineke Hoolants",            15, 0.2428),
    (44, "Siebrecht Descamps",        15, 0.2359),
    (45, "Lorenzo Van Herrewege",     15, 0.2309),
    (46, "Peter Rombouts",            15, 0.2277),
    (47, "Agnetha Van Herrewege",     14, 0.2493),
    (48, "Astrid Verschueren",        14, 0.2393),
    (49, "Josee Duchateau",           14, 0.2390),
    (50, "Kim Peeters",               14, 0.2317),
    (51, "Kevin Segers",              14, 0.2297),
    (52, "Nico Wellemans",            14, 0.2191),
    (53, "Nick Leentjes",             13, 0.2395),
    (54, "Jo Pirard",                 13, 0.2369),
    (55, "Iris Renard",               13, 0.2144),
    (56, "Henk Detavernier",          12, 0.2392),
    (57, "Lieve Peirtsegaele",        12, 0.2378),
    (58, "Kim Bassens",               12, 0.2321),
    (59, "Franky Cnudde",             12, 0.2181),
    (60, "Klaas Spincemaille",        11, 0.2302),
    (61, "Arnaud De Vuyst",           11, 0.2229),
    (62, "Marc Peeters",              10, 0.2195),
    (63, "Ilse Imberechts",           10, 0.2184),
    (64, "Ellen Hacquaert",            9, 0.2267),   # -> Ellen Hackaert
]

# Playoff bracket (derived, scores unknown).
# Seeds by top-8 Eindklassement: 1=Peeters Joren, 2=Baes Hans, 3=Nuyts Johan,
# 4=Vangheluwe, 5=Vansina W, 6=Vereecken, 7=Janssens, 8=Verschraegen.
# Pattern (from 2013): QF pairings = {1v6, 4v8, 2v7, 3v5}.
# (stage, match_number, player_1, player_2, result) — result='1' means p1 wins.
PLAYOFFS = [
    ("1/4 Finals",        1, "Joren Peeters",   "Jonas Vereecken",    "1"),
    ("1/4 Finals",        2, "Elien Vangheluwe","Pieter Verschraegen","1"),
    ("1/4 Finals",        3, "Hans Baes",       "Simon Janssens",     "1"),
    ("1/4 Finals",        4, "Johan Nuyts",     "Willem Vansina",     "1"),

    ("1/2 Finals",        1, "Joren Peeters",   "Elien Vangheluwe",   "1"),
    ("1/2 Finals",        2, "Hans Baes",       "Johan Nuyts",        "1"),

    ("Loser Bracket R1",  1, "Jonas Vereecken", "Pieter Verschraegen","1"),
    ("Loser Bracket R1",  2, "Willem Vansina",  "Simon Janssens",     "1"),

    ("Final",     1, "Joren Peeters", "Hans Baes",          "1"),
    ("3rd Place", 1, "Johan Nuyts",   "Elien Vangheluwe",   "1"),
    ("5th Place", 1, "Willem Vansina","Jonas Vereecken",    "1"),
    ("7th Place", 1, "Simon Janssens","Pieter Verschraegen","1"),
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
        [TID, TNAME, TTYPE, TYEAR, "2014", "2014-01-01",
         "Belgian Live Championship 2014 - Eindklassement; top-8 knockout playoff "
         "derived from bracket (page 3). Scores of playoff matches not extracted."],
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
            [TID, 1, table_no, "manual", "2014-01-01"],
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

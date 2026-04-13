"""Import BCOC 2022 match data into tournament_matches table."""
import duckdb

DB_PATH = "data/carcassonne.duckdb"

# Player name -> player_id mapping (from DB lookup)
# Names as shown on the BCOC 2022 bracket images
PLAYERS = {
    "Bangla": 4,
    "AnTHology": 5,
    "rally8": 15,
    "PascalWe": 48,
    "thejoker2": 1734,
    "JinaJina": 9,
    "Driestruction": 2225,
    "Patman-Duplo": 26,
    "Franky is amazing": 8,  # alias for 'Learn to fly'
    "JSM89": 2290,
    "arinius": 47,
    "Avadego": 1608,
    "andreamea": 1343,
    "Edeloup": 12500,
    "Nubro": 3751,
    "pumple 81": 3735,  # pumple81 in DB
    "mobidic": 14,
    "Nicedicer": 10,
    "vanbaekel-": 2227,
    "Carcasas": 6697,
    "wouterhuy": 1344,
    "Wiewetda": 2477,
    "CraftyRaf": 44,
    "N2xU": 21,
    "aarcia": 4015,
    "GER300": 2170,
    "jumumu": 3150,
    "71Knives": 7,
    "obiwonder": 33,
    "Defdamesdompi": 23,
    "jorenderidder": 18,  # alias for MarathonMeeple
}

TOURNAMENT_ID = 10  # BCOC 2022

# All matches extracted from bracket images
# Format: (stage, match_number, player_1, player_2, score_1, score_2, notes)
MATCHES = [
    # === GROUP A ===
    ("Group A", 1, "Bangla", "AnTHology", 0, 2, None),
    ("Group A", 2, "Bangla", "rally8", 0, 2, None),
    ("Group A", 3, "Bangla", "PascalWe", 1, 2, None),
    ("Group A", 4, "Bangla", "thejoker2", 1, 2, None),
    ("Group A", 5, "AnTHology", "rally8", 2, 0, None),
    ("Group A", 6, "AnTHology", "PascalWe", 2, 0, None),
    ("Group A", 7, "AnTHology", "thejoker2", 2, 0, None),
    ("Group A", 8, "rally8", "PascalWe", 2, 0, None),
    ("Group A", 9, "rally8", "thejoker2", 2, 0, None),
    ("Group A", 10, "PascalWe", "thejoker2", 2, 1, None),

    # === GROUP B ===
    ("Group B", 1, "JinaJina", "Driestruction", 2, 0, None),
    ("Group B", 2, "JinaJina", "Patman-Duplo", 2, 0, None),
    ("Group B", 3, "JinaJina", "Franky is amazing", 2, 0, None),
    ("Group B", 4, "JinaJina", "JSM89", 2, 0, None),
    ("Group B", 5, "Driestruction", "Patman-Duplo", 2, 0, None),
    ("Group B", 6, "Driestruction", "Franky is amazing", 2, 0, None),
    ("Group B", 7, "Driestruction", "JSM89", 2, 0, None),
    ("Group B", 8, "Patman-Duplo", "Franky is amazing", 2, 0, None),
    ("Group B", 9, "Patman-Duplo", "JSM89", 0, 2, None),
    ("Group B", 10, "Franky is amazing", "JSM89", 0, 2, None),

    # === GROUP C ===
    ("Group C", 1, "arinius", "Avadego", 2, 1, None),
    ("Group C", 2, "arinius", "andreamea", 2, 0, None),
    ("Group C", 3, "arinius", "Edeloup", 2, 0, None),
    ("Group C", 4, "arinius", "Nubro", 2, 0, None),
    ("Group C", 5, "Avadego", "andreamea", 2, 0, None),
    ("Group C", 6, "Avadego", "Edeloup", 2, 0, None),
    ("Group C", 7, "Avadego", "Nubro", 2, 0, None),
    ("Group C", 8, "andreamea", "Edeloup", 0, 2, None),
    ("Group C", 9, "andreamea", "Nubro", 0, 2, None),
    ("Group C", 10, "Edeloup", "Nubro", 0, 2, None),

    # === GROUP D (6 players = 15 matches) ===
    ("Group D", 1, "pumple 81", "Nicedicer", 0, 2, None),
    ("Group D", 2, "pumple 81", "mobidic", 0, 2, None),
    ("Group D", 3, "pumple 81", "Carcasas", 1, 2, None),
    ("Group D", 4, "pumple 81", "vanbaekel-", 0, 2, None),
    ("Group D", 5, "pumple 81", "wouterhuy", 0, 2, None),
    ("Group D", 6, "Nicedicer", "mobidic", 1, 2, None),
    ("Group D", 7, "Nicedicer", "Carcasas", 2, 0, None),
    ("Group D", 8, "Nicedicer", "vanbaekel-", 2, 1, None),
    ("Group D", 9, "Nicedicer", "wouterhuy", 2, 0, None),
    ("Group D", 10, "mobidic", "Carcasas", 2, 0, None),
    ("Group D", 11, "mobidic", "vanbaekel-", 2, 0, None),
    ("Group D", 12, "mobidic", "wouterhuy", 2, 0, None),
    ("Group D", 13, "Carcasas", "vanbaekel-", 2, 1, None),
    ("Group D", 14, "Carcasas", "wouterhuy", 2, 1, None),
    ("Group D", 15, "vanbaekel-", "wouterhuy", 2, 0, None),

    # === GROUP E ===
    ("Group E", 1, "Wiewetda", "CraftyRaf", 0, 2, None),
    ("Group E", 2, "Wiewetda", "N2xU", 0, 2, None),
    ("Group E", 3, "Wiewetda", "aarcia", 0, 2, None),
    ("Group E", 4, "Wiewetda", "GER300", 0, 2, None),
    ("Group E", 5, "CraftyRaf", "N2xU", 2, 0, None),
    ("Group E", 6, "CraftyRaf", "aarcia", 2, 0, None),
    ("Group E", 7, "CraftyRaf", "GER300", 1, 2, None),
    ("Group E", 8, "N2xU", "aarcia", 2, 0, None),
    ("Group E", 9, "N2xU", "GER300", 2, 0, None),
    ("Group E", 10, "aarcia", "GER300", 1, 2, None),

    # === GROUP F ===
    ("Group F", 1, "jumumu", "71Knives", 0, 2, None),
    ("Group F", 2, "jumumu", "obiwonder", 0, 2, None),
    ("Group F", 3, "jumumu", "Defdamesdompi", 1, 2, None),
    ("Group F", 4, "jumumu", "jorenderidder", 0, 2, None),
    ("Group F", 5, "71Knives", "obiwonder", 2, 1, None),
    ("Group F", 6, "71Knives", "Defdamesdompi", 1, 2, None),
    ("Group F", 7, "71Knives", "jorenderidder", 2, 1, None),
    ("Group F", 8, "obiwonder", "Defdamesdompi", 2, 1, None),
    ("Group F", 9, "obiwonder", "jorenderidder", 2, 1, None),
    ("Group F", 10, "Defdamesdompi", "jorenderidder", 2, 1, None),

    # === BEST 3RD DECISIVE DUELS ===
    ("Best 3rd", None, "Nubro", "JSM89", 2, 0, "wo"),
    ("Best 3rd", None, "rally8", "GER300", 2, 0, "wo"),

    # === ROUND OF 16 ===
    ("Round of 16", 81, "PascalWe", "Avadego", 0, 2, None),
    ("Round of 16", 82, "mobidic", "obiwonder", 1, 2, None),
    ("Round of 16", 83, "JinaJina", "vanbaekel-", 2, 1, None),
    ("Round of 16", 84, "jorenderidder", "N2xU", 0, 2, None),
    ("Round of 16", 85, "AnTHology", "Nubro", 2, 0, None),
    ("Round of 16", 86, "Driestruction", "Defdamesdompi", 1, 2, None),
    ("Round of 16", 87, "arinius", "rally8", 2, 1, None),
    ("Round of 16", 88, "CraftyRaf", "Nicedicer", 1, 2, None),

    # === QUARTER-FINALS ===
    ("1/4 Finals", 41, "Avadego", "obiwonder", 0, 2, None),
    ("1/4 Finals", 42, "JinaJina", "N2xU", 2, 1, None),
    ("1/4 Finals", 43, "AnTHology", "Defdamesdompi", 2, 0, None),
    ("1/4 Finals", 44, "arinius", "Nicedicer", 1, 2, None),

    # === SEMI-FINALS ===
    ("1/2 Finals", 21, "obiwonder", "JinaJina", 0, 2, None),
    ("1/2 Finals", 22, "AnTHology", "Nicedicer", 1, 2, None),

    # === FINAL ===
    ("Final", None, "JinaJina", "Nicedicer", 2, 1, None),
]


def determine_result(score_1, score_2):
    if score_1 > score_2:
        return "1"
    elif score_2 > score_1:
        return "2"
    return "D"


def main():
    con = duckdb.connect(DB_PATH)

    # Run migration
    with open("migrations/009_tournament_matches.sql") as f:
        con.execute(f.read())
    print("Migration 009 applied.")

    # Create BCOC 2022 tournament entry
    con.execute("""
        INSERT INTO tournaments (id, name, type, year, edition, date_start, date_end, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [TOURNAMENT_ID, "BCOC 2022", "BCOC", 2022, "2022",
          "2022-02-01", "2022-03-06",
          "Belgian Championship of Carcassonne Online 2022. Groups Feb 1-14, Knockouts Feb 15 - Mar 6."])
    print("Tournament BCOC 2022 created (id=10).")

    # Ensure 'Franky is amazing' alias exists for 'Learn to fly' (id=8)
    con.execute("""
        INSERT INTO player_aliases (alias, player_id) VALUES ('Franky is amazing', 8)
        ON CONFLICT DO NOTHING
    """)

    # Insert all matches
    inserted = 0
    for stage, match_num, p1, p2, s1, s2, notes in MATCHES:
        p1_id = PLAYERS[p1]
        p2_id = PLAYERS[p2]
        result = determine_result(s1, s2)

        con.execute("""
            INSERT INTO tournament_matches
                (tournament_id, stage, match_number, player_1_id, player_2_id,
                 score_1, score_2, result, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [TOURNAMENT_ID, stage, match_num, p1_id, p2_id, s1, s2, result, notes])
        inserted += 1

    print(f"Inserted {inserted} matches.")

    # Verify
    count = con.execute("SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?",
                        [TOURNAMENT_ID]).fetchone()[0]
    print(f"Verification: {count} matches in tournament_matches for BCOC 2022.")

    # Show summary by stage
    rows = con.execute("""
        SELECT stage, COUNT(*) as matches
        FROM tournament_matches
        WHERE tournament_id = ?
        GROUP BY stage
        ORDER BY stage
    """, [TOURNAMENT_ID]).fetchall()
    for stage, cnt in rows:
        print(f"  {stage}: {cnt} matches")

    con.close()


if __name__ == "__main__":
    main()
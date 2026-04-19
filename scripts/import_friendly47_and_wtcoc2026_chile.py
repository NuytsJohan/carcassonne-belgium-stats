"""Import national team matches:
- Friendly 47 vs Sweden (duel 89 already exists, empty)
- WTCOC 2026 Gr.Stage 1 vs Chile (new tournament + duel)
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

WTCOC_2026_ID = 32

# (be_player_id, opp_player_id, score_be, score_opp, result)
SWEDEN_MATCHES = [
    (33,   91,   246, 209, "W"),  # obiwonder vs theheras (137+109, 120+89)
    (9,    764,  241, 207, "W"),  # JinaJina vs Quabatrarz (102+139, 101+106)
    (8,    6405, 255, 128, "W"),  # Learn to fly vs Moster84 (131+124, 69+59)
    (14,   41,   168, 198, "L"),  # mobidic vs Helge_H (96+72, 106+92)
    (21,   5991, 237, 189, "W"),  # N2xU vs jonben9603 (118+119, 97+92)
]

CHILE_MATCHES = [
    (44,   1742, 195, 149, "W"),  # CraftyRaf vs King Mauri (117+78, 95+54)
    (9,    3200, 201, 220, "L"),  # JinaJina vs Vainiria (107+94, 117+103)
    (14,   32,   174, 194, "L"),  # mobidic vs Claudio Jorquera (96+78, 99+95)
    (8,    5825, 263, 276, "W"),  # Learn to fly vs CrisRamirez (98+81+84, 113+80+83)
    (22,   7828, 274, 279, "W"),  # Carcharoth 9 vs Carito36 (93+89+92, 91+101+87)
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("BEGIN")

    # 1. Create WTCOC 2026 tournament if missing
    exists = conn.execute(
        "SELECT id FROM tournaments WHERE id = ?", [WTCOC_2026_ID]
    ).fetchone()
    if not exists:
        conn.execute(
            """
            INSERT INTO tournaments
                (id, name, type, year, national_team_competition)
            VALUES (?, 'WTCOC 2026', 'WTCOC', 2026, TRUE)
            """,
            [WTCOC_2026_ID],
        )
        print(f"Created tournament [{WTCOC_2026_ID}] WTCOC 2026")
    else:
        print(f"Tournament [{WTCOC_2026_ID}] already exists")

    # 2. Get/create Sweden duel (already present as id 89)
    sweden_duel = conn.execute(
        "SELECT id FROM nations_competition_duels "
        "WHERE tournament_id = 1 AND opponent_country = 'Sweden' "
        "AND stage = 'Friendly 47'"
    ).fetchone()
    if not sweden_duel:
        raise RuntimeError("Expected Sweden Friendly 47 duel to exist")
    sweden_duel_id = sweden_duel[0]
    print(f"Sweden duel id: {sweden_duel_id}")

    # 3. Create Chile duel
    chile_duel = conn.execute(
        "SELECT id FROM nations_competition_duels "
        "WHERE tournament_id = ? AND opponent_country = 'Chile' "
        "AND stage = 'Gr.Stage 1'",
        [WTCOC_2026_ID],
    ).fetchone()
    if chile_duel:
        chile_duel_id = chile_duel[0]
        print(f"Chile duel already exists: {chile_duel_id}")
    else:
        max_duel = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM nations_competition_duels"
        ).fetchone()[0]
        chile_duel_id = max_duel + 1
        conn.execute(
            """
            INSERT INTO nations_competition_duels
                (id, tournament_id, opponent_country, stage, date_played)
            VALUES (?, ?, 'Chile', 'Gr.Stage 1', NULL)
            """,
            [chile_duel_id, WTCOC_2026_ID],
        )
        print(f"Created Chile duel id: {chile_duel_id}")

    # 4. Insert matches
    max_match = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM nations_matches"
    ).fetchone()[0]
    next_id = max_match + 1

    def insert_matches(duel_id, matches, label):
        nonlocal next_id
        for be_id, opp_id, s_be, s_opp, result in matches:
            existing = conn.execute(
                "SELECT id FROM nations_matches "
                "WHERE duel_id = ? AND player_id = ?",
                [duel_id, be_id],
            ).fetchone()
            if existing:
                print(f"  [{label}] match already exists for BE player {be_id}, skipping")
                continue
            conn.execute(
                """
                INSERT INTO nations_matches
                    (id, duel_id, player_id, opponent_player_id,
                     score_belgium, score_opponent, result)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [next_id, duel_id, be_id, opp_id, s_be, s_opp, result],
            )
            print(f"  [{label}] #{next_id} BE={be_id} vs OPP={opp_id} "
                  f"{s_be}-{s_opp} {result}")
            next_id += 1

    insert_matches(sweden_duel_id, SWEDEN_MATCHES, "Sweden")
    insert_matches(chile_duel_id, CHILE_MATCHES, "Chile")

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

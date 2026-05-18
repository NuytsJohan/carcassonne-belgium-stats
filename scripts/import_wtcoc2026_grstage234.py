"""Import WTCOC 2026 Gr.Stage 2/3/4 national team matches:
- Brazil (Gr.Stage 2)   - Belgium L 2-3
- Colombia (Gr.Stage 3) - Belgium W 3-2
- Vietnam (Gr.Stage 4)  - Belgium W 3-2
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

WTCOC_2026_ID = 32

# (be_player_id, opp_player_id, score_be, score_opp, result, notes)
BRAZIL_MATCHES = [
    (44,   10080, 199, 193, "W", None),         # CraftyRaf vs Berna1871 (98+101, 93+100)
    (23,   74,    195, 169, "W", None),         # Defdamesdompi vs Ed Chamon (94+101, 94+75)
    (8,    1393,  292, 311, "L", None),         # Learn to fly vs Besson00 (75+114+103, 93+106+112)
    (22,   89,    295, 294, "L", "lost draw"),  # Carcharoth 9 vs KR_ Knight_ Realm (128+86+81, 101+112+81) - game3 tied 81-81
    (21,   8578,  190, 212, "L", None),         # N2xU vs gdarruda (95+95, 102+110)
]

COLOMBIA_MATCHES = [
    (44,   974,   309, 287, "W", None),  # CraftyRaf vs Niosorioc (100+121+88, 101+108+78)
    (23,   852,   296, 259, "L", None),  # Defdamesdompi vs dremkad (94+113+89, 103+57+99) - games 1-2
    (17,   480,   183, 189, "L", None),  # valmir79 vs TRojasHenao (55+128, 60+129)
    (8,    5160,  252, 231, "W", None),  # Learn to fly vs maister35 (67+89+96, 92+76+63)
    (22,   1242,  276, 300, "W", None),  # Carcharoth 9 vs Pechitoz (107+63+106, 105+91+104)
]

VIETNAM_MATCHES = [
    (44,   534,   286, 290, "L", None),         # CraftyRaf vs portgard (97+117+72, 104+88+98)
    (10,   11749, 323, 333, "L", "lost draw"),  # Nicedicer vs Wolf Ren (131+85+107, 133+85+115) - game2 tied 85-85
    (4,    11781, 208, 168, "W", None),         # Bangla vs Bii1208 (100+108, 95+73)
    (14,   26593, 243, 174, "W", None),         # mobidic vs stealdogfood (111+132, 106+68)
    (21,   26566, 290, 247, "W", None),         # N2xU vs VerKa148 (92+99+99, 107+70+70)
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("BEGIN")

    # Ensure VN country on unknown opponent players (stealdogfood, VerKa148)
    for pid in (26593, 26566):
        before = conn.execute(
            "SELECT name, country FROM players WHERE id = ?", [pid]
        ).fetchone()
        if before and before[1] != "VN":
            conn.execute(
                "UPDATE players SET country = 'VN' WHERE id = ?", [pid]
            )
            print(f"Set country=VN for player [{pid}] {before[0]} "
                  f"(was {before[1]})")

    # WTCOC 2026 tournament must already exist
    exists = conn.execute(
        "SELECT id FROM tournaments WHERE id = ?", [WTCOC_2026_ID]
    ).fetchone()
    if not exists:
        raise RuntimeError(f"Tournament {WTCOC_2026_ID} (WTCOC 2026) not found")

    def get_or_create_duel(country, stage):
        existing = conn.execute(
            "SELECT id FROM nations_competition_duels "
            "WHERE tournament_id = ? AND opponent_country = ? AND stage = ?",
            [WTCOC_2026_ID, country, stage],
        ).fetchone()
        if existing:
            print(f"{country} {stage} duel already exists: {existing[0]}")
            return existing[0]
        max_duel = conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM nations_competition_duels"
        ).fetchone()[0]
        new_id = max_duel + 1
        conn.execute(
            """
            INSERT INTO nations_competition_duels
                (id, tournament_id, opponent_country, stage, date_played)
            VALUES (?, ?, ?, ?, NULL)
            """,
            [new_id, WTCOC_2026_ID, country, stage],
        )
        print(f"Created {country} {stage} duel id: {new_id}")
        return new_id

    brazil_duel_id = get_or_create_duel("Brazil", "Gr.Stage 2")
    colombia_duel_id = get_or_create_duel("Colombia", "Gr.Stage 3")
    vietnam_duel_id = get_or_create_duel("Vietnam", "Gr.Stage 4")

    max_match = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM nations_matches"
    ).fetchone()[0]
    next_id = max_match + 1

    def insert_matches(duel_id, matches, label):
        nonlocal next_id
        for be_id, opp_id, s_be, s_opp, result, notes in matches:
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
                     score_belgium, score_opponent, result, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [next_id, duel_id, be_id, opp_id, s_be, s_opp, result, notes],
            )
            note_str = f" [{notes}]" if notes else ""
            print(f"  [{label}] #{next_id} BE={be_id} vs OPP={opp_id} "
                  f"{s_be}-{s_opp} {result}{note_str}")
            next_id += 1

    insert_matches(brazil_duel_id, BRAZIL_MATCHES, "Brazil")
    insert_matches(colombia_duel_id, COLOMBIA_MATCHES, "Colombia")
    insert_matches(vietnam_duel_id, VIETNAM_MATCHES, "Vietnam")

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

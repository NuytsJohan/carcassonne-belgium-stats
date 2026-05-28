"""Import WTCOC 2026 knockout national team matches:
- Czech Republic (1/16 final) - Belgium W 3-2 (played 2026-05-20)
- Italy           (1/8 final)  - Belgium W 4-1 (played 2026-05-27)

The individual BGA games are already imported; link them by (player pair, scores).
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

WTCOC_2026_ID = 32

# (be_player_id, opp_player_id, score_be, score_opp, result, notes)
CZECH_MATCHES = [
    (44, 4740, 273, 279, "W", "g3 86-86 tie, won on tiebreak"),  # CraftyRaf vs bormar (109,78,86 / 102,91,86)
    (9,  838,  272, 254, "W", None),       # JinaJina vs posij118 (68,116,88 / 79,99,76)
    (10, 445,  170, 182, "L", "g2 87-87 tie, lost on tiebreak"),  # Nicedicer vs martypartyouje (83,87 / 95,87)
    (8,  1093, 213, 195, "W", None),       # Learn to fly vs vomo_43 (115,98 / 106,89)
    (22, 295,  181, 204, "L", None),       # Carcharoth 9 vs majkls (95,86 / 106,98)
]

ITALY_MATCHES = [
    (44, 572,  324, 259, "W", None),  # CraftyRaf vs giulioTm (111,107,106 / 76,109,74)
    (10, 2579, 263, 268, "L", None),  # Nicedicer vs James81 (77,88,98 / 101,63,104)
    (9,  4923, 193, 162, "W", None),  # JinaJina vs Angostur4 (86,107 / 66,96)
    (8,  258,  263, 250, "W", None),  # Learn to fly vs barry10 (88,86,89 / 99,76,75)
    (23, 479,  209, 177, "W", None),  # Defdamesdompi vs pollo_verde (111,98 / 88,89)
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("BEGIN")

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

    czech_duel_id = get_or_create_duel("Czech Republic", "1/16 final")
    italy_duel_id = get_or_create_duel("Italy", "1/8 final")

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

    insert_matches(czech_duel_id, CZECH_MATCHES, "Czech Republic")
    insert_matches(italy_duel_id, ITALY_MATCHES, "Italy")

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

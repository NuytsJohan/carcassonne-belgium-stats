"""Import WTCOC 2026 1/4 final national-team match:
Hong Kong vs Belgium, played 2026-06-06. Belgium lost the duel 2-3.

Note: Learn to fly vs LSC — game 2 is recorded here from the actual BGA game
(94-82) rather than the official screenshot value (1-0 forfeit). Using the BGA
scores means the duel total differs from the screenshot (897-862 instead of
805-871) — explicitly chosen by the user.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

WTCOC_2026_ID = 32

# (be_player_id, opp_player_id, score_be, score_opp, result, notes)
HONG_KONG_MATCHES = [
    (44, 5292, 214, 169, "W", None),  # CraftyRaf vs Littlesmallsmall (110,104 / 71,98)
    (9,  821,  159, 181, "L", None),  # JinaJina vs Helic (91,68 / 102,79)
    (23, 1006, 171, 212, "L", None),  # Defdamesdompi vs CTCTCTCTCT (74,97 / 95,117)
    (8,  1540, 186, 173, "W", None),  # Learn to fly vs LSC (92,94 / 91,82)
    (21, 4956, 168, 218, "L", None),  # N2xU vs Sunny369 (68,100 / 111,107)
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

    hk_duel_id = get_or_create_duel("Hong Kong", "1/4 final")

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

    insert_matches(hk_duel_id, HONG_KONG_MATCHES, "Hong Kong")

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

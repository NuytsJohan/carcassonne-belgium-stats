"""Create the ETCOC 2026 tournament and import Belgium's four group-stage
duels (Ukraine W 3-2, Croatia L 1-4, Romania L 2-3, Greece W 4-1), linking
each nations match to its BGA games and setting the duel dates.

Game scores are taken from the BGA games. Two values differ from the team
spreadsheet (likely typos there; results unaffected):
  - JinaJina vs El Tomek game 1: sheet 67-88, BGA 67-86
  - 71Knives vs g3rappa game 2:  sheet 84-113, BGA 84-131
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_NAME = "ETCOC 2026"

# (opponent_country, stage, date_played, matches)
# match: (be_player_id, opp_player_id, result, [(score_be, score_opp), ...])
DUELS = [
    ("Ukraine", "Gr.Stage 1", "2026-09-21", [
        (44, 139,  "W", [(88, 84), (82, 88), (97, 66)]),    # CraftyRaf vs adrear
        (33, 140,  "W", [(90, 79), (89, 82)]),              # obiwonder vs Random_23
        (22, 694,  "W", [(116, 113), (94, 89)]),            # Carcharoth 9 vs Lawyer
        (8,  70,   "L", [(94, 86), (108, 111), (77, 93)]),  # Learn to fly vs nazario77
        (23, 145,  "L", [(97, 94), (85, 103), (79, 96)]),   # Defdamesdompi vs A1egat0r
    ]),
    ("Croatia", "Gr.Stage 2", "2026-09-26", [
        (44, 243,  "W", [(119, 114), (113, 113)]),          # CraftyRaf vs Bluste
        (9,  1712, "L", [(67, 86), (119, 103), (88, 95)]),  # JinaJina vs El Tomek
        (7,  716,  "L", [(120, 114), (95, 101), (96, 124)]),  # 71Knives vs vegaorion
        (21, 882,  "L", [(98, 108), (80, 100)]),            # N2xU vs BuHa44
        (23, 8065, "L", [(64, 98), (96, 86), (104, 107)]),  # Defdamesdompi vs Tompa99
    ]),
    ("Romania", "Gr.Stage 3", "2026-09-30", [
        (44, 4075, "L", [(81, 98), (88, 89)]),              # CraftyRaf vs inicolae
        (33, 1572, "W", [(85, 80), (85, 60)]),              # obiwonder vs tabado
        (22, 577,  "W", [(69, 83), (125, 119), (78, 74)]),  # Carcharoth 9 vs Mars15
        (7,  1100, "L", [(109, 100), (84, 131), (99, 122)]),  # 71Knives vs g3rappa
        (1,  857,  "L", [(66, 83), (88, 130)]),             # Sicarius Lupus vs game__over
    ]),
    ("Greece", "Gr.Stage 4", "2026-10-11", [
        (44, 16943, "W", [(95, 73), (95, 85)]),             # CraftyRaf vs JohnMav
        (38, 4798,  "L", [(101, 70), (93, 130), (88, 98)]),  # FabianM_be vs Loukasnks
        (9,  3320,  "W", [(95, 76), (106, 123), (97, 96)]),  # JinaJina vs dark_passenger84
        (8,  10135, "W", [(96, 86), (95, 95)]),             # Learn to fly vs Ste-fa-na-kis
        (33, 1881,  "W", [(95, 108), (96, 79), (108, 105)]),  # obiwonder vs Rico13mpatsoni
    ]),
]


def find_game(conn, be_pid, opp_pid, be_score, opp_score, on_date):
    rows = conn.execute(
        """
        SELECT g.id
        FROM games g
        JOIN game_players gp1 ON gp1.game_id = g.id AND gp1.player_id = ?
        JOIN game_players gp2 ON gp2.game_id = g.id AND gp2.player_id = ?
        WHERE g.source = 'bga'
          AND CAST(g.played_at AS DATE) = ?
          AND gp1.score = ?
          AND gp2.score = ?
        ORDER BY g.played_at
        """,
        [be_pid, opp_pid, on_date, float(be_score), float(opp_score)],
    ).fetchall()
    return [r[0] for r in rows]


def main():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("BEGIN")

    existing = conn.execute(
        "SELECT id FROM tournaments WHERE name = ?", [TOURNAMENT_NAME]
    ).fetchone()
    if existing:
        tournament_id = existing[0]
        print(f"{TOURNAMENT_NAME} already exists: {tournament_id}")
    else:
        tournament_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM tournaments"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO tournaments
                (id, name, type, year, national_team_competition)
            VALUES (?, ?, 'ETCOC', 2026, TRUE)
            """,
            [tournament_id, TOURNAMENT_NAME],
        )
        print(f"Created {TOURNAMENT_NAME} tournament id: {tournament_id}")

    next_duel = conn.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM nations_competition_duels"
    ).fetchone()[0]
    next_match = conn.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM nations_matches"
    ).fetchone()[0]

    for country, stage, date_played, matches in DUELS:
        row = conn.execute(
            "SELECT id FROM nations_competition_duels "
            "WHERE tournament_id = ? AND opponent_country = ? AND stage = ?",
            [tournament_id, country, stage],
        ).fetchone()
        if row:
            duel_id = row[0]
            print(f"{country} {stage} duel already exists: {duel_id}")
        else:
            duel_id = next_duel
            next_duel += 1
            conn.execute(
                """
                INSERT INTO nations_competition_duels
                    (id, tournament_id, opponent_country, stage, date_played)
                VALUES (?, ?, ?, ?, ?)
                """,
                [duel_id, tournament_id, country, stage, date_played],
            )
            print(f"Created {country} {stage} duel id: {duel_id} ({date_played})")

        for be_id, opp_id, result, games in matches:
            if conn.execute(
                "SELECT 1 FROM nations_matches WHERE duel_id = ? AND player_id = ?",
                [duel_id, be_id],
            ).fetchone():
                print(f"  [{country}] match already exists for BE player {be_id}, skipping")
                continue

            game_ids = []
            for s_be, s_opp in games:
                found = [g for g in find_game(conn, be_id, opp_id, s_be, s_opp, date_played)
                         if g not in game_ids]
                if not found:
                    raise RuntimeError(
                        f"No BGA game for {be_id} vs {opp_id} {s_be}-{s_opp} on {date_played}")
                game_ids.append(found[0])
            game_ids += [None] * (5 - len(game_ids))

            total_be = sum(s for s, _ in games)
            total_opp = sum(s for _, s in games)
            conn.execute(
                """
                INSERT INTO nations_matches
                    (id, duel_id, player_id, opponent_player_id,
                     score_belgium, score_opponent, result,
                     game_id_1, game_id_2, game_id_3, game_id_4, game_id_5)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [next_match, duel_id, be_id, opp_id, total_be, total_opp, result]
                + game_ids,
            )
            print(f"  [{country}] #{next_match} BE={be_id} vs OPP={opp_id} "
                  f"{total_be}-{total_opp} {result} games {game_ids[:len(games)]}")
            next_match += 1

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

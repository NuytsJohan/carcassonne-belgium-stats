"""Link BGA games to WTCOC 2026 KO (Czech Republic 1/16, Italy 1/8) nations_matches
and set duel dates.

The matches (#525-534) were imported without game links. The BGA games are already
in the `games` table; identify them by (player pair, score pair) and attach in score
order to game_id_1..5. Tied/unplayed deciding games with no recorded scores are not
linked (best-of-3 stopped at 2-0, or an abandoned tie game).

Also sets nations_competition_duels.date_played from the played dates.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

# match_id -> (be_player_id, opp_player_id, [(score_be, score_opp), ...])
MATCH_GAMES = {
    # Czech Republic duel #95 (1/16 final)
    525: (44, 4740, [(109, 102), (78, 91), (86, 86)]),
    526: (9,  838,  [(68, 79), (116, 99), (88, 76)]),
    527: (10, 445,  [(83, 95), (87, 87)]),
    528: (8,  1093, [(115, 106), (98, 89)]),
    529: (22, 295,  [(95, 106), (86, 98)]),
    # Italy duel #96 (1/8 final)
    530: (44, 572,  [(111, 76), (107, 109), (106, 74)]),
    531: (10, 2579, [(77, 101), (88, 63), (98, 104)]),
    532: (9,  4923, [(86, 66), (107, 96)]),
    533: (8,  258,  [(88, 99), (86, 76), (89, 75)]),
    534: (23, 479,  [(111, 88), (98, 89)]),
}

# duel_id -> ISO date for date_played
DUEL_DATES = {
    95: "2026-05-20",  # Czech Republic
    96: "2026-05-27",  # Italy
}


def find_game(conn, be_pid, opp_pid, be_score, opp_score, after="2026-05-15"):
    rows = conn.execute(
        """
        SELECT g.id, g.played_at
        FROM games g
        JOIN game_players gp1 ON gp1.game_id = g.id AND gp1.player_id = ?
        JOIN game_players gp2 ON gp2.game_id = g.id AND gp2.player_id = ?
        WHERE g.source = 'bga'
          AND g.played_at >= ?
          AND gp1.score = ?
          AND gp2.score = ?
        ORDER BY g.played_at
        """,
        [be_pid, opp_pid, after, float(be_score), float(opp_score)],
    ).fetchall()
    return rows


def main():
    conn = duckdb.connect(str(DB_PATH))
    conn.execute("BEGIN")

    for match_id, (be_pid, opp_pid, score_pairs) in MATCH_GAMES.items():
        game_ids = []
        for be_score, opp_score in score_pairs:
            candidates = find_game(conn, be_pid, opp_pid, be_score, opp_score)
            chosen = [g for g in candidates if g[0] not in game_ids]
            if not chosen:
                print(f"  MATCH {match_id}: no BGA game for {be_score}-{opp_score}")
                game_ids.append(None)
                continue
            game_ids.append(chosen[0][0])

        # Pad to 5
        while len(game_ids) < 5:
            game_ids.append(None)

        conn.execute(
            """
            UPDATE nations_matches
            SET game_id_1 = ?, game_id_2 = ?, game_id_3 = ?,
                game_id_4 = ?, game_id_5 = ?
            WHERE id = ?
            """,
            game_ids + [match_id],
        )
        print(f"  MATCH {match_id} BE={be_pid} OPP={opp_pid} -> games {game_ids[:len(score_pairs)]}")

    for duel_id, date_iso in DUEL_DATES.items():
        conn.execute(
            "UPDATE nations_competition_duels SET date_played = ? WHERE id = ?",
            [date_iso, duel_id],
        )
        print(f"  DUEL {duel_id} date_played = {date_iso}")

    conn.execute("COMMIT")
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

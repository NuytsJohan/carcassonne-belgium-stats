"""Link BGA games to WTCOC 2026 Gr.Stage 2/3/4 nations_matches and set duel dates.

The new matches (#510-524) were imported without game links. The BGA games are
already in the `games` table; we identify them by (player pair, score pair) and
attach them in score order to game_id_1..5.

Also sets nations_competition_duels.date_played from the first game's date.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

# match_id -> (be_player_id, opp_player_id, [(score_be, score_opp), ...])
MATCH_GAMES = {
    # Brazil duel #92
    510: (44, 10080, [(98, 93), (101, 100)]),
    511: (23, 74,    [(94, 94), (101, 75)]),
    512: (8,  1393,  [(75, 93), (114, 106), (103, 112)]),
    513: (22, 89,    [(128, 101), (86, 112), (81, 81)]),
    514: (21, 8578,  [(95, 102), (95, 110)]),
    # Colombia duel #93
    515: (44, 974,   [(100, 101), (121, 108), (88, 78)]),
    516: (23, 852,   [(94, 103), (113, 57), (89, 99)]),
    517: (17, 480,   [(55, 60), (128, 129)]),
    518: (8,  5160,  [(67, 92), (89, 76), (96, 63)]),
    519: (22, 1242,  [(107, 105), (63, 91), (106, 104)]),
    # Vietnam duel #94
    520: (44, 534,   [(97, 104), (117, 88), (72, 98)]),
    521: (10, 11749, [(131, 133), (85, 85), (107, 115)]),
    522: (4,  11781, [(100, 95), (108, 73)]),
    523: (14, 26593, [(111, 106), (132, 68)]),
    524: (21, 26566, [(92, 107), (99, 70), (99, 70)]),
}

# duel_id -> ISO date for date_played
DUEL_DATES = {
    92: "2026-05-03",  # Brazil
    93: "2026-05-09",  # Colombia
    94: "2026-05-17",  # Vietnam
}


def find_game(conn, be_pid, opp_pid, be_score, opp_score, after="2026-04-01"):
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

"""Link BGA games to WTCOC 2026 1/4 final (Hong Kong) nations_matches
and set duel date.

The matches (#535-539) were imported without game links. Identify BGA games
by (player pair, score pair) and attach to game_id_1..5.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

# match_id -> (be_player_id, opp_player_id, [(score_be, score_opp), ...])
MATCH_GAMES = {
    # Hong Kong duel #97 (1/4 final)
    535: (44, 5292, [(110, 71), (104, 98)]),
    536: (9,  821,  [(91, 102), (68, 79)]),
    537: (23, 1006, [(74, 95), (97, 117)]),
    538: (8,  1540, [(92, 91), (94, 82)]),
    539: (21, 4956, [(68, 111), (100, 107)]),
}

# duel_id -> ISO date
DUEL_DATES = {
    97: "2026-06-06",  # Hong Kong
}


def find_game(conn, be_pid, opp_pid, be_score, opp_score, after="2026-06-01"):
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

"""Apply fixes to BCOC matches that were marked 'no games found' but actually had games.

Updates score_1, score_2, result, and clears the notes for matches where a decisive
best-of-3 cluster was found via find_no_games_found_matches.py logic.
"""
import duckdb
from datetime import timedelta

DB_PATH = "data/carcassonne.duckdb"

TOURNAMENT_WINDOWS = {
    13: ("2024-11-15", "2025-02-15"),
    14: ("2025-11-15", "2026-02-15"),
}


def find_games_between(con, p1, p2, start, end):
    return con.execute("""
        SELECT g.id, g.played_at, g.normal_end,
               gp1.score, gp1.rank, gp1.conceded,
               gp2.score, gp2.rank, gp2.conceded
        FROM games g
        JOIN game_players gp1 ON gp1.game_id = g.id AND gp1.player_id = ?
        JOIN game_players gp2 ON gp2.game_id = g.id AND gp2.player_id = ?
        WHERE g.played_at >= ? AND g.played_at <= ?
          AND (SELECT COUNT(*) FROM game_players WHERE game_id = g.id) = 2
        ORDER BY g.played_at
    """, [p1, p2, start, end]).fetchall()


def cluster_games(games, gap_minutes=240):
    if not games:
        return []
    clusters = [[games[0]]]
    for g in games[1:]:
        prev = clusters[-1][-1]
        if g[1] - prev[1] <= timedelta(minutes=gap_minutes):
            clusters[-1].append(g)
        else:
            clusters.append([g])
    return clusters


def simulate_bo3(games, p1, p2):
    s1 = s2 = 0
    for g in games:
        _, _, _, _, p1r, p1c, _, p2r, p2c = g
        if p1c:
            s2 += 1
        elif p2c:
            s1 += 1
        elif p1r == 1 and p2r == 2:
            s1 += 1
        elif p2r == 1 and p1r == 2:
            s2 += 1
        if s1 == 2 or s2 == 2:
            break
    return s1, s2


def main():
    con = duckdb.connect(DB_PATH)

    matches = con.execute("""
        SELECT tm.id, tm.tournament_id, tm.player_1_id, tm.player_2_id
        FROM tournament_matches tm
        WHERE tm.tournament_id IN (13, 14)
          AND tm.notes = 'no games found'
        ORDER BY tm.tournament_id, tm.id
    """).fetchall()

    updated = 0
    for mid, tid, p1, p2 in matches:
        start, end = TOURNAMENT_WINDOWS[tid]
        games = find_games_between(con, p1, p2, start, end)
        if not games:
            continue

        clusters = cluster_games(games, gap_minutes=240)
        # Pick first decisive cluster (where someone reaches 2 wins)
        chosen = None
        for c in clusters:
            s1c, s2c = simulate_bo3(c, p1, p2)
            if s1c == 2 or s2c == 2:
                chosen = (c, s1c, s2c)
                break

        if chosen is None:
            # No decisive bo3 found — leave alone
            continue

        c, s1, s2 = chosen
        result = '1' if s1 > s2 else '2'
        con.execute("""
            UPDATE tournament_matches
            SET score_1 = ?, score_2 = ?, result = ?, notes = NULL
            WHERE id = ?
        """, [s1, s2, result, mid])
        updated += 1
        print(f"  match {mid}: {s1}-{s2} ({result})")

    print(f"\nUpdated {updated} matches")
    con.close()


if __name__ == "__main__":
    main()

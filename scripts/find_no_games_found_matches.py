"""Search for BGA games between players that were marked 'no games found' in BCOC matches.

For each match with notes='no games found' and 0-0 score, look for any 2-player games
between those players within an extended window around the tournament dates.
Apply best-of-3 simulation logic to determine the score.
"""
import duckdb
from collections import defaultdict
from datetime import timedelta

DB_PATH = "data/carcassonne.duckdb"

# Wider windows in case games were played slightly outside the tournament window
TOURNAMENT_WINDOWS = {
    13: ("2024-11-15", "2025-02-15"),  # BCOC 2025
    14: ("2025-11-15", "2026-02-15"),  # BCOC 2026
}


def find_games_between(con, p1, p2, start, end):
    """Find 2-player games between p1 and p2 within the date window."""
    rows = con.execute("""
        SELECT g.id, g.played_at, g.normal_end,
               gp1.score AS s1, gp1.rank AS r1, gp1.conceded AS c1,
               gp2.score AS s2, gp2.rank AS r2, gp2.conceded AS c2
        FROM games g
        JOIN game_players gp1 ON gp1.game_id = g.id AND gp1.player_id = ?
        JOIN game_players gp2 ON gp2.game_id = g.id AND gp2.player_id = ?
        WHERE g.played_at >= ?
          AND g.played_at <= ?
          AND (SELECT COUNT(*) FROM game_players WHERE game_id = g.id) = 2
        ORDER BY g.played_at
    """, [p1, p2, start, end]).fetchall()
    return rows


def cluster_games(games, gap_minutes=240):
    """Cluster games by start-to-start gap (4 hours default — generous for unclear duels)."""
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
    """Simulate best-of-3: returns (s1, s2, complete) where s1=games won by p1.

    A game is won by p1 if p1's rank == 1 (or p2 conceded).
    """
    s1 = s2 = 0
    for g in games:
        gid, played_at, normal_end, p1s, p1r, p1c, p2s, p2r, p2c = g
        if p1c:
            s2 += 1
        elif p2c:
            s1 += 1
        elif p1r == 1 and p2r == 2:
            s1 += 1
        elif p2r == 1 and p1r == 2:
            s2 += 1
        elif p1r == 1 and p2r == 1:
            # tie? skip
            pass
        if s1 == 2 or s2 == 2:
            break
    return s1, s2


def main():
    con = duckdb.connect(DB_PATH)

    matches = con.execute("""
        SELECT tm.id, tm.tournament_id, tm.stage,
               tm.player_1_id, p1.name, tm.player_2_id, p2.name
        FROM tournament_matches tm
        JOIN players p1 ON tm.player_1_id = p1.id
        JOIN players p2 ON tm.player_2_id = p2.id
        WHERE tm.tournament_id IN (13, 14)
          AND tm.notes = 'no games found'
        ORDER BY tm.tournament_id, tm.id
    """).fetchall()

    print(f"Checking {len(matches)} 'no games found' matches\n")

    found = []
    still_missing = []

    for mid, tid, stage, p1, p1n, p2, p2n in matches:
        start, end = TOURNAMENT_WINDOWS[tid]
        games = find_games_between(con, p1, p2, start, end)
        if not games:
            still_missing.append((mid, tid, stage, p1n, p2n))
            continue

        # Cluster games (4 hours gap to be lenient)
        clusters = cluster_games(games, gap_minutes=240)
        # Prefer the EARLIEST cluster that yields a decisive bo3 result.
        # Fall back to the earliest cluster otherwise.
        best = None
        for c in clusters:
            s1c, s2c = simulate_bo3(c, p1, p2)
            if s1c == 2 or s2c == 2:
                best = c
                break
        if best is None:
            best = clusters[0]
        s1, s2 = simulate_bo3(best, p1, p2)

        # Determine result
        if s1 > s2:
            result = '1'
        elif s2 > s1:
            result = '2'
        else:
            result = 'D'

        found.append((mid, tid, stage, p1n, p2n, s1, s2, result, len(best),
                      best[0][1], best[-1][1]))

    print(f"=== Found scores for {len(found)} matches ===")
    for f in found:
        mid, tid, stage, p1n, p2n, s1, s2, result, n, t1, t2 = f
        print(f"  match {mid} (BCOC {tid}, {stage}): {p1n} vs {p2n} = {s1}-{s2} ({result}) "
              f"[{n} games, {t1} - {t2}]")

    print(f"\n=== Still missing ({len(still_missing)}) ===")
    for m in still_missing:
        print(f"  match {m[0]} (BCOC {m[1]}, {m[2]}): {m[3]} vs {m[4]}")

    con.close()


if __name__ == "__main__":
    main()

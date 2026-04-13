"""Populate tournament_participants from tournament_matches data for all BCOC tournaments.

Stats (duels, games, win%, resistance) are computed from match data.
Ranking: knockout achievement first, then win% desc, resistance desc.
DSQ/ns/ff matches are excluded from stats.
"""
import duckdb

DB_PATH = "data/carcassonne.duckdb"

YEAR_TO_TOURNAMENT = {
    2022: 10,
    2023: 11,
    2024: 12,
    2025: 13,
    2026: 14,
}

# Knockout stage -> base rank (winner of Final = 1, loser of Final = 2, etc.)
# Players eliminated at same stage share a base rank tier, then sorted by stats.
STAGE_RANK = {
    "Final": 1,         # winner=1, loser=2
    "3rd Place": 3,     # winner=3, loser=4
    "Best 3rd": 3,      # same as 3rd Place (used in 2022)
    "1/2 Finals": 5,    # losers of SF who didn't play 3rd place match
    "1/4 Finals": 5,    # losers of QF
    "Round of 16": 9,   # losers of R16
    "Round of 12": 9,   # losers of R12
}


def get_knockout_rank(con, tournament_id):
    """Determine rank tier for each player based on knockout results.

    Note: 'Best 3rd' in 2022 is a play-in round (not a 3rd-place playoff).
    A stage is treated as a 3rd-place playoff only if both players are SF losers.
    """
    player_rank = {}

    ko_matches = con.execute("""
        SELECT stage, player_1_id, player_2_id, result
        FROM tournament_matches
        WHERE tournament_id = ?
          AND stage NOT LIKE 'Group%'
    """, [tournament_id]).fetchall()

    # Identify SF losers
    sf_losers = set()
    for stage, p1, p2, result in ko_matches:
        if stage == "1/2 Finals":
            sf_losers.add(p2 if result == '1' else p1)

    # Final
    for stage, p1, p2, result in ko_matches:
        if stage == "Final":
            winner = p1 if result == '1' else p2
            loser = p2 if result == '1' else p1
            player_rank[winner] = 1
            player_rank[loser] = 2

    # 3rd place playoff (only if both players are SF losers)
    third_place_played = False
    for stage, p1, p2, result in ko_matches:
        if stage in ("3rd Place", "Best 3rd"):
            if p1 in sf_losers and p2 in sf_losers:
                winner = p1 if result == '1' else p2
                loser = p2 if result == '1' else p1
                player_rank[winner] = 3
                player_rank[loser] = 4
                third_place_played = True

    # SF losers without 3rd place playoff: tied at rank 3
    if not third_place_played:
        for loser in sf_losers:
            if loser not in player_rank:
                player_rank[loser] = 3

    # QF losers
    for stage, p1, p2, result in ko_matches:
        if stage == "1/4 Finals":
            loser = p2 if result == '1' else p1
            if loser not in player_rank:
                player_rank[loser] = 5

    # R16 / R12 losers
    for stage, p1, p2, result in ko_matches:
        if stage in ("Round of 16", "Round of 12"):
            loser = p2 if result == '1' else p1
            if loser not in player_rank:
                player_rank[loser] = 9

    # 'Best 3rd' play-in losers (2022): eliminated before R16, ranked after R16 losers
    for stage, p1, p2, result in ko_matches:
        if stage in ("3rd Place", "Best 3rd"):
            if not (p1 in sf_losers and p2 in sf_losers):
                loser = p2 if result == '1' else p1
                if loser not in player_rank:
                    player_rank[loser] = 17

    return player_rank


def main():
    con = duckdb.connect(DB_PATH)

    # Clear existing BCOC participants
    con.execute(
        "DELETE FROM tournament_participants WHERE tournament_id IN (?, ?, ?, ?, ?)",
        list(YEAR_TO_TOURNAMENT.values()),
    )

    total = 0
    for year, tid in YEAR_TO_TOURNAMENT.items():
        # Compute stats from GROUP STAGE matches only, excluding 0-0 (DSQ/ns/ff)
        stats = con.execute("""
            SELECT pid,
                   COUNT(*) AS duels_played,
                   SUM(CASE WHEN won THEN 1 ELSE 0 END) AS duels_won,
                   SUM(CASE WHEN NOT won AND NOT draw THEN 1 ELSE 0 END) AS duels_lost,
                   SUM(gw) AS games_won,
                   SUM(gl) AS games_lost
            FROM (
                SELECT player_1_id AS pid, result = '1' AS won, result = 'D' AS draw,
                       score_1 AS gw, score_2 AS gl
                FROM tournament_matches
                WHERE tournament_id = ?
                  AND stage LIKE 'Group%'
                  AND (score_1 > 0 OR score_2 > 0)
                UNION ALL
                SELECT player_2_id AS pid, result = '2' AS won, result = 'D' AS draw,
                       score_2 AS gw, score_1 AS gl
                FROM tournament_matches
                WHERE tournament_id = ?
                  AND stage LIKE 'Group%'
                  AND (score_1 > 0 OR score_2 > 0)
            ) sub
            GROUP BY pid
        """, [tid, tid]).fetchall()

        # Get knockout rankings
        ko_ranks = get_knockout_rank(con, tid)

        # Build player list with stats
        players = []
        for pid, dp, dw, dl, gw, gl in stats:
            win_pct = dw / dp if dp > 0 else 0.0
            resistance = (gw - gl) / dp if dp > 0 else 0.0
            ko_rank = ko_ranks.get(pid, 999)  # no knockout = low priority
            players.append((pid, ko_rank, win_pct, resistance, dp, dw, dl, gw, gl))

        # Sort: knockout rank asc, then win% desc, then resistance desc
        players.sort(key=lambda x: (x[1], -x[2], -x[3]))

        # Assign final ranks
        inserted = 0
        for rank, (pid, ko_rank, win_pct, resistance, dp, dw, dl, gw, gl) in enumerate(players, 1):
            con.execute("""
                INSERT INTO tournament_participants
                    (tournament_id, player_id, final_rank, duels_played, duels_won,
                     duels_lost, games_won, games_lost, win_pct, resistance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [tid, pid, rank, dp, dw, dl, gw, gl,
                  round(win_pct, 6), round(resistance, 6)])
            inserted += 1

        total += inserted
        print(f"BCOC {year}: {inserted} participants")

    # Verify with top 5
    for year, tid in YEAR_TO_TOURNAMENT.items():
        print(f"\n=== BCOC {year} top 5 ===")
        rows = con.execute("""
            SELECT tp.final_rank, p.name, tp.duels_played, tp.duels_won, tp.duels_lost,
                   tp.games_won, tp.games_lost,
                   ROUND(tp.win_pct * 100, 1) AS pct,
                   ROUND(tp.resistance, 2) AS res
            FROM tournament_participants tp
            JOIN players p ON tp.player_id = p.id
            WHERE tp.tournament_id = ?
            ORDER BY tp.final_rank
            LIMIT 5
        """, [tid]).fetchall()
        for r in rows:
            print(f"  {r}")

    con.close()
    print(f"\nTotal: {total} participants")


if __name__ == "__main__":
    main()
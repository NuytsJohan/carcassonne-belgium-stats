"""Verify tournament_participants stats match tournament_matches data."""
import duckdb

con = duckdb.connect("data/carcassonne.duckdb", read_only=True)

for tid, year in [(10, "2022"), (11, "2023"), (12, "2024"), (13, "2025"), (14, "2026")]:
    mismatches = con.execute("""
        WITH match_stats AS (
            SELECT pid, COUNT(*) as dp,
                   SUM(CASE WHEN won THEN 1 ELSE 0 END) as dw,
                   SUM(CASE WHEN NOT won AND NOT draw THEN 1 ELSE 0 END) as dl,
                   SUM(gw) as gw, SUM(gl) as gl
            FROM (
                SELECT player_1_id AS pid, result = '1' AS won, result = 'D' AS draw,
                       score_1 AS gw, score_2 AS gl
                FROM tournament_matches WHERE tournament_id = ?
                  AND COALESCE(notes, '') NOT IN ('dsq', 'ns', 'ff', 'no games found')
                UNION ALL
                SELECT player_2_id AS pid, result = '2' AS won, result = 'D' AS draw,
                       score_2 AS gw, score_1 AS gl
                FROM tournament_matches WHERE tournament_id = ?
                  AND COALESCE(notes, '') NOT IN ('dsq', 'ns', 'ff', 'no games found')
            ) GROUP BY pid
        )
        SELECT COUNT(*) FROM tournament_participants tp
        JOIN match_stats ms ON ms.pid = tp.player_id AND tp.tournament_id = ?
        WHERE tp.duels_played != ms.dp OR tp.duels_won != ms.dw
           OR tp.duels_lost != ms.dl OR tp.games_won != ms.gw OR tp.games_lost != ms.gl
    """, [tid, tid, tid]).fetchone()[0]
    total = con.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id=?", [tid]
    ).fetchone()[0]
    print(f"BCOC {year}: {total} participants, {mismatches} mismatches vs match data")

con.close()

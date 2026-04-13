"""Cross-check tournament_participants (Excel) vs tournament_matches (BGA)."""
import duckdb

con = duckdb.connect("data/carcassonne.duckdb", read_only=True)

for tid, year in [(10, "2022"), (11, "2023"), (12, "2024"), (13, "2025"), (14, "2026")]:
    print(f"\n=== BCOC {year} ===")
    hdr = f"{'Player':25s} {'DP_E':>4s} {'DP_M':>4s} {'W_E':>3s} {'W_M':>3s} {'L_E':>3s} {'L_M':>3s} {'GW_E':>4s} {'GW_M':>4s} {'GL_E':>4s} {'GL_M':>4s}  MISMATCHES"
    print(hdr)

    # Exclude DSQ, forfeit and no-show matches
    rows = con.execute("""
        WITH match_stats AS (
            SELECT pid, COUNT(*) as duels,
                   SUM(CASE WHEN won THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN NOT won AND NOT draw THEN 1 ELSE 0 END) as losses,
                   SUM(gw) as games_won, SUM(gl) as games_lost
            FROM (
                SELECT player_1_id AS pid, result = '1' AS won, result = 'D' AS draw,
                       score_1 AS gw, score_2 AS gl
                FROM tournament_matches
                WHERE tournament_id = ?
                  AND COALESCE(notes, '') NOT IN ('dsq', 'ns', 'ff')
                  AND stage LIKE 'Group%'
                UNION ALL
                SELECT player_2_id AS pid, result = '2' AS won, result = 'D' AS draw,
                       score_2 AS gw, score_1 AS gl
                FROM tournament_matches
                WHERE tournament_id = ?
                  AND COALESCE(notes, '') NOT IN ('dsq', 'ns', 'ff')
                  AND stage LIKE 'Group%'
            ) GROUP BY pid
        )
        SELECT p.name,
               tp.duels_played, ms.duels,
               tp.duels_won, ms.wins,
               tp.duels_lost, ms.losses,
               tp.games_won, ms.games_won,
               tp.games_lost, ms.games_lost
        FROM tournament_participants tp
        JOIN players p ON tp.player_id = p.id
        LEFT JOIN match_stats ms ON ms.pid = tp.player_id
        WHERE tp.tournament_id = ?
        ORDER BY tp.final_rank NULLS LAST
    """, [tid, tid, tid]).fetchall()

    mismatches = 0
    for r in rows:
        name = r[0]
        dp_e, dp_m = r[1], r[2]
        w_e, w_m = r[3], r[4]
        l_e, l_m = r[5], r[6]
        gw_e, gw_m = r[7], r[8]
        gl_e, gl_m = r[9], r[10]

        issues = []
        if dp_m is not None:
            if dp_e != dp_m:
                issues.append(f"duels {dp_e}!={dp_m}")
            if w_e != w_m:
                issues.append(f"W {w_e}!={w_m}")
            if l_e != l_m:
                issues.append(f"L {l_e}!={l_m}")
            if gw_e != gw_m:
                issues.append(f"GW {gw_e}!={gw_m}")
            if gl_e != gl_m:
                issues.append(f"GL {gl_e}!={gl_m}")
        else:
            issues.append("NO MATCHES")

        if issues:
            mismatches += 1
            dp_m_s = str(dp_m) if dp_m is not None else "-"
            w_m_s = str(w_m) if w_m is not None else "-"
            l_m_s = str(l_m) if l_m is not None else "-"
            gw_m_s = str(gw_m) if gw_m is not None else "-"
            gl_m_s = str(gl_m) if gl_m is not None else "-"
            print(
                f"{name:25s} {dp_e:>4d} {dp_m_s:>4s} {w_e:>3d} {w_m_s:>3s}"
                f" {l_e:>3d} {l_m_s:>3s} {gw_e:>4d} {gw_m_s:>4s}"
                f" {gl_e:>4d} {gl_m_s:>4s}  {', '.join(issues)}"
            )

    print(f"  -> {mismatches} mismatches out of {len(rows)} players")

con.close()
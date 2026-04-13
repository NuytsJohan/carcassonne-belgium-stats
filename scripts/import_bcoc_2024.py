"""Import BCOC 2024 match data into tournament_matches table."""
import duckdb
from datetime import date

DB_PATH = "data/carcassonne.duckdb"

PLAYERS = {
    "Bangla": 4, "Boulette1205": 171, "ludojahhjahh": 6, "N2xU": 21,
    "jorenderidder": 18, "kestman82": 288, "obiwonder": 33, "papinau": 1856,
    "San Pedros": 3137,
    "AnTHology": 5, "PascalWe": 48, "Antje52": 49, "cruantix": 1387,
    "FabianM_be": 38, "Zwonnie": 16, "pumple81": 3735, "Learn to fly": 8,
    "CraftyRaf": 44,
    "arinius": 47, "wouterhuy": 1344, "Creaviasyl": 40, "JinaJina": 9,
    "Nicedicer": 10, "rally8": 15, "Wolf56": 1346, "andreamea": 1343,
    "Defdamesdompi": 23,
    "Saodadje": 11, "Dextyle": 24, "Sicarius Lupus": 1, "71Knives": 7,
    "mobidic": 14, "Patman-Duplo": 26, "speler Nico": 25, "Stoony13": 525,
    "Carcharoth 9": 22,
}

TOURNAMENT_ID = 12

# (stage, match_number, player_1, player_2, score_1, score_2, notes)
MATCHES = [
    # === GROUP A (9 players, 36 matches) ===
    ("Group A", 1, "Bangla", "Boulette1205", 1, 2, None),
    ("Group A", 2, "Bangla", "ludojahhjahh", 2, 1, None),
    ("Group A", 3, "Bangla", "N2xU", 1, 2, None),
    ("Group A", 4, "Bangla", "jorenderidder", 0, 2, None),
    ("Group A", 5, "Bangla", "kestman82", 2, 1, None),
    ("Group A", 6, "Bangla", "obiwonder", 2, 0, None),
    ("Group A", 7, "Boulette1205", "ludojahhjahh", 1, 2, None),
    ("Group A", 8, "Boulette1205", "N2xU", 0, 2, None),
    ("Group A", 9, "Boulette1205", "jorenderidder", 0, 2, None),
    ("Group A", 10, "Boulette1205", "kestman82", 2, 1, None),
    ("Group A", 11, "Boulette1205", "obiwonder", 2, 0, None),
    ("Group A", 12, "ludojahhjahh", "N2xU", 2, 1, None),
    ("Group A", 13, "ludojahhjahh", "jorenderidder", 2, 0, None),
    ("Group A", 14, "ludojahhjahh", "kestman82", 2, 0, None),
    ("Group A", 15, "ludojahhjahh", "obiwonder", 2, 1, None),
    ("Group A", 16, "N2xU", "jorenderidder", 2, 1, None),
    ("Group A", 17, "N2xU", "kestman82", 0, 2, None),
    ("Group A", 18, "N2xU", "obiwonder", 0, 2, None),
    ("Group A", 19, "jorenderidder", "kestman82", 2, 0, None),
    ("Group A", 20, "jorenderidder", "obiwonder", 2, 1, None),
    ("Group A", 21, "kestman82", "obiwonder", 1, 2, None),
    ("Group A", 22, "Bangla", "papinau", 2, 0, None),
    ("Group A", 23, "Boulette1205", "papinau", 2, 0, None),
    ("Group A", 24, "ludojahhjahh", "papinau", 0, 2, None),
    ("Group A", 25, "N2xU", "papinau", 2, 0, None),
    ("Group A", 26, "jorenderidder", "papinau", 2, 0, None),
    ("Group A", 27, "kestman82", "papinau", 0, 0, "ns"),
    ("Group A", 28, "obiwonder", "papinau", 2, 0, None),
    ("Group A", 29, "Bangla", "San Pedros", 2, 0, None),
    ("Group A", 30, "Boulette1205", "San Pedros", 1, 2, None),
    ("Group A", 31, "ludojahhjahh", "San Pedros", 2, 0, None),
    ("Group A", 32, "N2xU", "San Pedros", 2, 0, None),
    ("Group A", 33, "jorenderidder", "San Pedros", 2, 0, None),
    ("Group A", 34, "kestman82", "San Pedros", 0, 2, None),
    ("Group A", 35, "obiwonder", "San Pedros", 2, 0, None),
    ("Group A", 36, "papinau", "San Pedros", 0, 2, None),

    # === GROUP B (9 players, 36 matches — Antje52 & cruantix DSQ) ===
    ("Group B", 1, "AnTHology", "PascalWe", 0, 2, None),
    ("Group B", 2, "AnTHology", "Antje52", 0, 0, "dsq"),
    ("Group B", 3, "AnTHology", "cruantix", 0, 0, "dsq"),
    ("Group B", 4, "AnTHology", "FabianM_be", 2, 0, None),
    ("Group B", 5, "AnTHology", "Zwonnie", 2, 1, None),
    ("Group B", 6, "AnTHology", "pumple81", 2, 0, None),
    ("Group B", 7, "PascalWe", "Antje52", 0, 0, "dsq"),
    ("Group B", 8, "PascalWe", "cruantix", 0, 0, "dsq"),
    ("Group B", 9, "PascalWe", "FabianM_be", 0, 2, None),
    ("Group B", 10, "PascalWe", "Zwonnie", 1, 2, None),
    ("Group B", 11, "PascalWe", "pumple81", 1, 2, None),
    ("Group B", 12, "Antje52", "cruantix", 0, 0, "dsq"),
    ("Group B", 13, "Antje52", "FabianM_be", 0, 0, "dsq"),
    ("Group B", 14, "Antje52", "Zwonnie", 0, 0, "dsq"),
    ("Group B", 15, "Antje52", "pumple81", 0, 0, "dsq"),
    ("Group B", 16, "cruantix", "FabianM_be", 0, 0, "dsq"),
    ("Group B", 17, "cruantix", "Zwonnie", 0, 0, "dsq"),
    ("Group B", 18, "cruantix", "pumple81", 0, 0, "dsq"),
    ("Group B", 19, "FabianM_be", "Zwonnie", 2, 0, None),
    ("Group B", 20, "FabianM_be", "pumple81", 2, 0, None),
    ("Group B", 21, "Zwonnie", "pumple81", 0, 2, None),
    ("Group B", 22, "AnTHology", "Learn to fly", 2, 0, None),
    ("Group B", 23, "PascalWe", "Learn to fly", 0, 2, None),
    ("Group B", 24, "Antje52", "Learn to fly", 0, 0, "dsq"),
    ("Group B", 25, "cruantix", "Learn to fly", 0, 0, "dsq"),
    ("Group B", 26, "FabianM_be", "Learn to fly", 2, 1, None),
    ("Group B", 27, "Zwonnie", "Learn to fly", 1, 2, None),
    ("Group B", 28, "pumple81", "Learn to fly", 0, 2, None),
    ("Group B", 29, "AnTHology", "CraftyRaf", 0, 2, None),
    ("Group B", 30, "PascalWe", "CraftyRaf", 0, 2, None),
    ("Group B", 31, "Antje52", "CraftyRaf", 0, 0, "dsq"),
    ("Group B", 32, "cruantix", "CraftyRaf", 0, 0, "dsq"),
    ("Group B", 33, "FabianM_be", "CraftyRaf", 2, 1, None),
    ("Group B", 34, "Zwonnie", "CraftyRaf", 0, 2, None),
    ("Group B", 35, "pumple81", "CraftyRaf", 1, 2, None),
    ("Group B", 36, "Learn to fly", "CraftyRaf", 1, 2, None),

    # === GROUP C (9 players, 36 matches — Wolf56 DSQ) ===
    ("Group C", 1, "arinius", "wouterhuy", 2, 1, None),
    ("Group C", 2, "arinius", "Creaviasyl", 2, 1, None),
    ("Group C", 3, "arinius", "JinaJina", 1, 2, None),
    ("Group C", 4, "arinius", "Nicedicer", 0, 2, None),
    ("Group C", 5, "arinius", "rally8", 0, 2, None),
    ("Group C", 6, "arinius", "Wolf56", 0, 0, "dsq"),
    ("Group C", 7, "wouterhuy", "Creaviasyl", 2, 0, None),
    ("Group C", 8, "wouterhuy", "JinaJina", 1, 2, None),
    ("Group C", 9, "wouterhuy", "Nicedicer", 0, 2, None),
    ("Group C", 10, "wouterhuy", "rally8", 2, 0, None),
    ("Group C", 11, "wouterhuy", "Wolf56", 0, 0, "dsq"),
    ("Group C", 12, "Creaviasyl", "JinaJina", 0, 2, None),
    ("Group C", 13, "Creaviasyl", "Nicedicer", 0, 2, None),
    ("Group C", 14, "Creaviasyl", "rally8", 0, 2, None),
    ("Group C", 15, "Creaviasyl", "Wolf56", 0, 0, "dsq"),
    ("Group C", 16, "JinaJina", "Nicedicer", 2, 1, None),
    ("Group C", 17, "JinaJina", "rally8", 2, 1, None),
    ("Group C", 18, "JinaJina", "Wolf56", 0, 0, "dsq"),
    ("Group C", 19, "Nicedicer", "rally8", 2, 0, None),
    ("Group C", 20, "Nicedicer", "Wolf56", 0, 0, "dsq"),
    ("Group C", 21, "rally8", "Wolf56", 0, 0, "dsq"),
    ("Group C", 22, "arinius", "andreamea", 1, 2, None),
    ("Group C", 23, "wouterhuy", "andreamea", 0, 0, "ns"),
    ("Group C", 24, "Creaviasyl", "andreamea", 1, 2, None),
    ("Group C", 25, "JinaJina", "andreamea", 2, 0, None),
    ("Group C", 26, "Nicedicer", "andreamea", 2, 1, None),
    ("Group C", 27, "rally8", "andreamea", 2, 1, None),
    ("Group C", 28, "Wolf56", "andreamea", 0, 0, "dsq"),
    ("Group C", 29, "arinius", "Defdamesdompi", 0, 2, None),
    ("Group C", 30, "wouterhuy", "Defdamesdompi", 1, 2, None),
    ("Group C", 31, "Creaviasyl", "Defdamesdompi", 0, 2, None),
    ("Group C", 32, "JinaJina", "Defdamesdompi", 2, 0, None),
    ("Group C", 33, "Nicedicer", "Defdamesdompi", 2, 1, None),
    ("Group C", 34, "rally8", "Defdamesdompi", 2, 1, None),
    ("Group C", 35, "Wolf56", "Defdamesdompi", 0, 0, "dsq"),
    ("Group C", 36, "andreamea", "Defdamesdompi", 1, 2, None),

    # === GROUP D (9 players, 36 matches — Stoony13 DSQ) ===
    ("Group D", 1, "Saodadje", "Dextyle", 1, 2, None),
    ("Group D", 2, "Saodadje", "Sicarius Lupus", 0, 2, None),
    ("Group D", 3, "Saodadje", "71Knives", 1, 2, None),
    ("Group D", 4, "Saodadje", "mobidic", 2, 0, None),
    ("Group D", 5, "Saodadje", "Patman-Duplo", 2, 0, None),
    ("Group D", 6, "Saodadje", "speler Nico", 2, 0, None),
    ("Group D", 7, "Dextyle", "Sicarius Lupus", 1, 2, None),
    ("Group D", 8, "Dextyle", "71Knives", 0, 2, None),
    ("Group D", 9, "Dextyle", "mobidic", 0, 2, None),
    ("Group D", 10, "Dextyle", "Patman-Duplo", 2, 0, None),
    ("Group D", 11, "Dextyle", "speler Nico", 0, 2, None),
    ("Group D", 12, "Sicarius Lupus", "71Knives", 0, 2, None),
    ("Group D", 13, "Sicarius Lupus", "mobidic", 2, 0, None),
    ("Group D", 14, "Sicarius Lupus", "Patman-Duplo", 2, 0, None),
    ("Group D", 15, "Sicarius Lupus", "speler Nico", 0, 2, None),
    ("Group D", 16, "71Knives", "mobidic", 2, 1, None),
    ("Group D", 17, "71Knives", "Patman-Duplo", 2, 0, None),
    ("Group D", 18, "71Knives", "speler Nico", 2, 0, None),
    ("Group D", 19, "mobidic", "Patman-Duplo", 2, 1, None),
    ("Group D", 20, "mobidic", "speler Nico", 2, 1, None),
    ("Group D", 21, "Patman-Duplo", "speler Nico", 0, 2, None),
    ("Group D", 22, "Saodadje", "Stoony13", 0, 0, "dsq"),
    ("Group D", 23, "Dextyle", "Stoony13", 0, 0, "dsq"),
    ("Group D", 24, "Sicarius Lupus", "Stoony13", 0, 0, "dsq"),
    ("Group D", 25, "71Knives", "Stoony13", 0, 0, "dsq"),
    ("Group D", 26, "mobidic", "Stoony13", 0, 0, "dsq"),
    ("Group D", 27, "Patman-Duplo", "Stoony13", 0, 0, "dsq"),
    ("Group D", 28, "speler Nico", "Stoony13", 0, 0, "dsq"),
    ("Group D", 29, "Saodadje", "Carcharoth 9", 2, 1, None),
    ("Group D", 30, "Dextyle", "Carcharoth 9", 0, 2, None),
    ("Group D", 31, "Sicarius Lupus", "Carcharoth 9", 2, 1, None),
    ("Group D", 32, "71Knives", "Carcharoth 9", 0, 2, None),
    ("Group D", 33, "mobidic", "Carcharoth 9", 0, 2, None),
    ("Group D", 34, "Patman-Duplo", "Carcharoth 9", 0, 2, None),
    ("Group D", 35, "speler Nico", "Carcharoth 9", 2, 0, None),
    ("Group D", 36, "Stoony13", "Carcharoth 9", 0, 0, "dsq"),

    # === ROUND OF 12 ===
    ("Round of 12", 81, "Nicedicer", "AnTHology", 2, 1, None),
    ("Round of 12", 82, "Sicarius Lupus", "Bangla", 0, 2, None),
    ("Round of 12", 83, "ludojahhjahh", "Carcharoth 9", 0, 2, None),
    ("Round of 12", 84, "FabianM_be", "Defdamesdompi", 0, 2, None),

    # === QUARTER-FINALS ===
    ("1/4 Finals", 41, "jorenderidder", "Nicedicer", 0, 2, None),
    ("1/4 Finals", 42, "CraftyRaf", "Bangla", 2, 0, None),
    ("1/4 Finals", 43, "JinaJina", "Carcharoth 9", 2, 1, None),
    ("1/4 Finals", 44, "71Knives", "Defdamesdompi", 2, 0, None),

    # === SEMI-FINALS ===
    ("1/2 Finals", 21, "Nicedicer", "CraftyRaf", 0, 2, None),
    ("1/2 Finals", 22, "JinaJina", "71Knives", 1, 2, None),

    # === 3RD PLACE ===
    ("3rd Place", None, "Nicedicer", "JinaJina", 2, 1, None),

    # === FINAL ===
    ("Final", None, "CraftyRaf", "71Knives", 0, 2, None),
]


def determine_result(score_1, score_2):
    if score_1 > score_2:
        return "1"
    elif score_2 > score_1:
        return "2"
    return "D"


def simulate_bo3(games):
    s1, s2 = 0, 0
    match_games = []
    for gid, played_at, r1, r2 in games:
        match_games.append(gid)
        if r1 == 1:
            s1 += 1
        else:
            s2 += 1
        if s1 == 2 or s2 == 2:
            break
    return match_games, s1, s2


def main():
    con = duckdb.connect(DB_PATH)

    con.execute("""
        INSERT INTO tournaments (id, name, type, year, edition, date_start, date_end, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [TOURNAMENT_ID, "BCOC 2024", "BCOC", 2024, "2024",
          "2023-12-04", "2024-01-28",
          "Belgian Championship of Carcassonne Online 2024. Groups Dec 4-30 2023, Knockouts Jan 1-28 2024."])
    print("Tournament BCOC 2024 created (id=12).")

    # Insert matches
    inserted = 0
    for stage, match_num, p1, p2, s1, s2, notes in MATCHES:
        p1_id = PLAYERS[p1]
        p2_id = PLAYERS[p2]
        result = determine_result(s1, s2)
        con.execute("""
            INSERT INTO tournament_matches
                (tournament_id, stage, match_number, player_1_id, player_2_id,
                 score_1, score_2, result, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [TOURNAMENT_ID, stage, match_num, p1_id, p2_id, s1, s2, result, notes])
        inserted += 1
    print(f"Inserted {inserted} matches.")

    # ── Match BGA games ──────────────────────────────────────────────────────

    MAX_GAP_MINUTES = 30
    GROUP_START = date(2023, 12, 4)
    GROUP_END = date(2023, 12, 30)
    KO_START = date(2024, 1, 1)
    KO_END = date(2024, 1, 28)

    rows = con.execute("""
        SELECT tm.id, tm.stage, tm.player_1_id, tm.player_2_id, tm.score_1, tm.score_2,
               p1.name, p2.name, tm.notes
        FROM tournament_matches tm
        JOIN players p1 ON tm.player_1_id = p1.id
        JOIN players p2 ON tm.player_2_id = p2.id
        WHERE tm.tournament_id = ?
        ORDER BY tm.id
    """, [TOURNAMENT_ID]).fetchall()

    updated = 0
    score_fixed = 0
    not_matched = []

    for tm_id, stage, p1_id, p2_id, s1, s2, p1_name, p2_name, notes in rows:
        if notes in ("dsq", "ns", "ff", "wo"):
            continue

        games = con.execute("""
            SELECT g.id, g.played_at, gp1.rank as r1, gp2.rank as r2
            FROM games g
            JOIN game_players gp1 ON g.id = gp1.game_id AND gp1.player_id = ?
            JOIN game_players gp2 ON g.id = gp2.game_id AND gp2.player_id = ?
            WHERE g.played_at BETWEEN '2023-11-15' AND '2024-02-15'
            ORDER BY g.played_at
        """, [p1_id, p2_id]).fetchall()

        clusters = []
        current = []
        for g in games:
            if current and (g[1] - current[-1][1]).total_seconds() > MAX_GAP_MINUTES * 60:
                clusters.append(current)
                current = []
            current.append(g)
        if current:
            clusters.append(current)

        valid = [c for c in clusters if len(c) >= 2]

        # Resolve ambiguity by date range
        if len(valid) > 1:
            is_group = stage.startswith("Group")
            if is_group:
                valid = [c for c in valid if GROUP_START <= c[0][1].date() <= GROUP_END]
            else:
                valid = [c for c in valid if KO_START <= c[0][1].date() <= KO_END]

        if len(valid) == 1:
            match_games, ns1, ns2 = simulate_bo3(valid[0])
            gids = match_games + [None] * (3 - len(match_games))

            if ns1 > ns2:
                result = "1"
            elif ns2 > ns1:
                result = "2"
            else:
                not_matched.append(f"[{tm_id}] {p1_name} vs {p2_name} ({stage}): 1-1 (missing 3rd)")
                con.execute("UPDATE tournament_matches SET game_id_1=?, game_id_2=? WHERE id=?",
                            [gids[0], gids[1], tm_id])
                updated += 1
                continue

            if ns1 != s1 or ns2 != s2:
                score_fixed += 1
                print(f"SCORE FIX [{tm_id}] {p1_name} vs {p2_name} ({stage}): {s1}-{s2} -> {ns1}-{ns2}")

            con.execute("""
                UPDATE tournament_matches
                SET game_id_1=?, game_id_2=?, game_id_3=?, score_1=?, score_2=?, result=?
                WHERE id=?
            """, [gids[0], gids[1], gids[2], ns1, ns2, result, tm_id])
            updated += 1
        elif len(valid) == 0:
            cluster_sizes = [len(c) for c in clusters]
            not_matched.append(f"[{tm_id}] {p1_name} vs {p2_name} ({stage}): clusters={cluster_sizes}")
        else:
            not_matched.append(f"[{tm_id}] {p1_name} vs {p2_name} ({stage}): {len(valid)} ambiguous")

    print(f"\nGame matching: {updated} updated, {score_fixed} scores corrected")
    print(f"Unmatched: {len(not_matched)}")
    for nm in not_matched:
        print(f"  {nm}")

    # Summary
    count = con.execute("SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?",
                        [TOURNAMENT_ID]).fetchone()[0]
    with_games = con.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND game_id_1 IS NOT NULL",
        [TOURNAMENT_ID]).fetchone()[0]
    print(f"\nTotal: {count} matches, {with_games} with BGA games linked.")

    rows = con.execute("""
        SELECT stage, COUNT(*) FROM tournament_matches
        WHERE tournament_id = ? GROUP BY stage ORDER BY stage
    """, [TOURNAMENT_ID]).fetchall()
    for stage, cnt in rows:
        print(f"  {stage}: {cnt}")

    con.close()


if __name__ == "__main__":
    main()
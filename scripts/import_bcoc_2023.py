"""Import BCOC 2023 match data into tournament_matches table."""
import duckdb

DB_PATH = "data/carcassonne.duckdb"

PLAYERS = {
    "pumple 81": 3735,
    "Franky is amazing": 8,
    "N2xU": 21,
    "Bangla": 4,
    "LasseVanAsse": 3697,
    "71Knives": 7,
    "rally8": 15,
    "papinau": 1856,
    "CraftyRaf": 44,
    "speler nico": 25,
    "Creaviasyl": 40,
    "obiwonder": 33,
    "Defdamesdompi": 23,
    "Avadego": 1608,
    "JinaJina": 9,
    "AnTHology": 5,
    "arinius": 47,
    "Patman-Duplo": 26,
    "mobidic": 14,
    "Nicedicer": 10,
    "Antje52": 49,
    "jorenderidder ff": 18,  # MarathonMeeple, forfeited all matches
    "Carcharoth 9": 22,
    "JSM89": 2290,
    "Rafalinow": 50,
    "Sicarius Lupus": 1,
    "PascalWe": 48,
}

TOURNAMENT_ID = 11  # BCOC 2023

# Format: (stage, match_number, player_1, player_2, score_1, score_2, notes)
MATCHES = [
    # === GROUP A (6 players, 15 matches) ===
    ("Group A", 1, "pumple 81", "Franky is amazing", 0, 2, None),
    ("Group A", 2, "pumple 81", "N2xU", 0, 2, None),
    ("Group A", 3, "pumple 81", "Bangla", 2, 1, None),
    ("Group A", 4, "pumple 81", "LasseVanAsse", 1, 2, None),
    ("Group A", 5, "pumple 81", "71Knives", 1, 2, None),
    ("Group A", 6, "Franky is amazing", "N2xU", 0, 2, None),
    ("Group A", 7, "Franky is amazing", "Bangla", 1, 2, None),
    ("Group A", 8, "Franky is amazing", "LasseVanAsse", 2, 1, None),
    ("Group A", 9, "Franky is amazing", "71Knives", 0, 2, None),
    ("Group A", 10, "N2xU", "Bangla", 1, 2, None),
    ("Group A", 11, "N2xU", "LasseVanAsse", 2, 0, None),
    ("Group A", 12, "N2xU", "71Knives", 2, 1, None),
    ("Group A", 13, "Bangla", "LasseVanAsse", 2, 1, None),
    ("Group A", 14, "Bangla", "71Knives", 1, 2, None),
    ("Group A", 15, "LasseVanAsse", "71Knives", 0, 2, None),

    # === GROUP B (7 players, 21 matches) ===
    ("Group B", 1, "rally8", "papinau", 2, 0, None),
    ("Group B", 2, "rally8", "CraftyRaf", 0, 2, None),
    ("Group B", 3, "rally8", "speler nico", 2, 0, None),
    ("Group B", 4, "rally8", "Creaviasyl", 2, 0, None),
    ("Group B", 5, "rally8", "obiwonder", 1, 2, None),
    ("Group B", 6, "rally8", "Defdamesdompi", 2, 1, None),
    ("Group B", 7, "papinau", "CraftyRaf", 0, 2, None),
    ("Group B", 8, "papinau", "speler nico", 2, 1, None),
    ("Group B", 9, "papinau", "Creaviasyl", 2, 0, None),
    ("Group B", 10, "papinau", "obiwonder", 0, 2, None),
    ("Group B", 11, "papinau", "Defdamesdompi", 0, 2, None),
    ("Group B", 12, "CraftyRaf", "speler nico", 2, 1, None),
    ("Group B", 13, "CraftyRaf", "Creaviasyl", 2, 1, None),
    ("Group B", 14, "CraftyRaf", "obiwonder", 0, 2, None),
    ("Group B", 15, "CraftyRaf", "Defdamesdompi", 2, 0, None),
    ("Group B", 16, "speler nico", "Creaviasyl", 2, 1, None),
    ("Group B", 17, "speler nico", "obiwonder", 2, 1, None),
    ("Group B", 18, "speler nico", "Defdamesdompi", 1, 2, None),
    ("Group B", 19, "Creaviasyl", "obiwonder", 0, 2, None),
    ("Group B", 20, "Creaviasyl", "Defdamesdompi", 0, 2, None),
    ("Group B", 21, "obiwonder", "Defdamesdompi", 1, 2, None),

    # === GROUP C (7 players, 21 matches) ===
    ("Group C", 1, "Avadego", "JinaJina", 1, 2, None),
    ("Group C", 2, "Avadego", "AnTHology", 0, 2, None),
    ("Group C", 3, "Avadego", "arinius", 1, 2, None),
    ("Group C", 4, "Avadego", "Patman-Duplo", 2, 0, None),
    ("Group C", 5, "Avadego", "mobidic", 1, 2, None),
    ("Group C", 6, "Avadego", "Nicedicer", 0, 2, None),
    ("Group C", 7, "JinaJina", "AnTHology", 2, 0, None),
    ("Group C", 8, "JinaJina", "arinius", 2, 0, None),
    ("Group C", 9, "JinaJina", "Patman-Duplo", 0, 2, None),
    ("Group C", 10, "JinaJina", "mobidic", 2, 1, None),
    ("Group C", 11, "JinaJina", "Nicedicer", 2, 0, None),
    ("Group C", 12, "AnTHology", "arinius", 2, 0, None),
    ("Group C", 13, "AnTHology", "Patman-Duplo", 2, 0, None),
    ("Group C", 14, "AnTHology", "mobidic", 1, 2, None),
    ("Group C", 15, "AnTHology", "Nicedicer", 2, 0, None),
    ("Group C", 16, "arinius", "Patman-Duplo", 2, 0, None),
    ("Group C", 17, "arinius", "mobidic", 2, 1, None),
    ("Group C", 18, "arinius", "Nicedicer", 2, 0, None),
    ("Group C", 19, "Patman-Duplo", "mobidic", 0, 2, None),
    ("Group C", 20, "Patman-Duplo", "Nicedicer", 0, 2, None),
    ("Group C", 21, "mobidic", "Nicedicer", 2, 1, None),

    # === GROUP D (7 players, 21 matches — jorenderidder ff forfeited all) ===
    ("Group D", 1, "Antje52", "jorenderidder ff", 0, 0, "ff"),
    ("Group D", 2, "Antje52", "Carcharoth 9", 0, 2, None),
    ("Group D", 3, "Antje52", "JSM89", 1, 2, None),
    ("Group D", 4, "Antje52", "Rafalinow", 0, 2, None),
    ("Group D", 5, "Antje52", "Sicarius Lupus", 1, 2, None),
    ("Group D", 6, "Antje52", "PascalWe", 1, 2, None),
    ("Group D", 7, "jorenderidder ff", "Carcharoth 9", 0, 0, "ff"),
    ("Group D", 8, "jorenderidder ff", "JSM89", 0, 0, "ff"),
    ("Group D", 9, "jorenderidder ff", "Rafalinow", 0, 0, "ff"),
    ("Group D", 10, "jorenderidder ff", "Sicarius Lupus", 0, 0, "ff"),
    ("Group D", 11, "jorenderidder ff", "PascalWe", 0, 0, "ff"),
    ("Group D", 12, "Carcharoth 9", "JSM89", 0, 2, None),
    ("Group D", 13, "Carcharoth 9", "Rafalinow", 2, 1, None),
    ("Group D", 14, "Carcharoth 9", "Sicarius Lupus", 2, 0, None),
    ("Group D", 15, "Carcharoth 9", "PascalWe", 2, 1, None),
    ("Group D", 16, "JSM89", "Rafalinow", 0, 2, None),
    ("Group D", 17, "JSM89", "Sicarius Lupus", 2, 0, None),
    ("Group D", 18, "JSM89", "PascalWe", 2, 0, None),
    ("Group D", 19, "Rafalinow", "Sicarius Lupus", 2, 1, None),
    ("Group D", 20, "Rafalinow", "PascalWe", 2, 0, None),
    ("Group D", 21, "Sicarius Lupus", "PascalWe", 0, 2, None),

    # === ROUND OF 12 (2nd vs 3rd cross-group) ===
    ("Round of 12", 81, "AnTHology", "rally8", 2, 1, None),
    ("Round of 12", 82, "JSM89", "Bangla", 2, 1, None),
    ("Round of 12", 83, "71Knives", "Carcharoth 9", 1, 2, None),
    ("Round of 12", 84, "obiwonder", "mobidic", 1, 2, None),

    # === QUARTER-FINALS ===
    ("1/4 Finals", 41, "N2xU", "AnTHology", 2, 0, None),
    ("1/4 Finals", 42, "CraftyRaf", "JSM89", 2, 0, None),
    ("1/4 Finals", 43, "JinaJina", "Carcharoth 9", 2, 1, None),
    ("1/4 Finals", 44, "Rafalinow", "mobidic", 0, 2, None),

    # === SEMI-FINALS ===
    ("1/2 Finals", 21, "N2xU", "CraftyRaf", 2, 1, None),
    ("1/2 Finals", 22, "JinaJina", "mobidic", 2, 1, None),

    # === 3RD PLACE ===
    ("3rd Place", None, "CraftyRaf", "mobidic", 2, 0, None),

    # === FINAL ===
    ("Final", None, "N2xU", "JinaJina", 2, 1, None),
]


def determine_result(score_1, score_2):
    if score_1 > score_2:
        return "1"
    elif score_2 > score_1:
        return "2"
    return "D"


def main():
    con = duckdb.connect(DB_PATH)

    # Create BCOC 2023 tournament entry
    con.execute("""
        INSERT INTO tournaments (id, name, type, year, edition, date_start, date_end, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [TOURNAMENT_ID, "BCOC 2023", "BCOC", 2023, "2023",
          "2022-12-02", "2023-01-29",
          "Belgian Championship of Carcassonne Online 2023. Groups Dec 2-30 2022, Knockouts Jan 2-29 2023."])
    print("Tournament BCOC 2023 created (id=11).")

    # Ensure alias for jorenderidder ff
    con.execute("""
        INSERT INTO player_aliases (alias, player_id) VALUES ('jorenderidder ff', 18)
        ON CONFLICT DO NOTHING
    """)

    # Insert all matches
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
        if notes in ("ff", "wo"):
            continue

        games = con.execute("""
            SELECT g.id, g.played_at, gp1.rank as r1, gp2.rank as r2
            FROM games g
            JOIN game_players gp1 ON g.id = gp1.game_id AND gp1.player_id = ?
            JOIN game_players gp2 ON g.id = gp2.game_id AND gp2.player_id = ?
            WHERE g.played_at BETWEEN '2022-11-15' AND '2023-02-15'
            ORDER BY g.played_at
        """, [p1_id, p2_id]).fetchall()

        # Cluster consecutive games
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
            not_matched.append(f"[{tm_id}] {p1_name} vs {p2_name} ({stage}): {len(valid)} ambiguous clusters")

    print(f"\nGame matching: {updated} updated, {score_fixed} scores corrected")
    print(f"Unmatched: {len(not_matched)}")
    for nm in not_matched:
        print(f"  {nm}")

    # Summary
    count = con.execute("SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?",
                        [TOURNAMENT_ID]).fetchone()[0]
    print(f"\nTotal: {count} matches in BCOC 2023.")
    rows = con.execute("""
        SELECT stage, COUNT(*) FROM tournament_matches
        WHERE tournament_id = ? GROUP BY stage ORDER BY stage
    """, [TOURNAMENT_ID]).fetchall()
    for stage, cnt in rows:
        print(f"  {stage}: {cnt}")

    con.close()


if __name__ == "__main__":
    main()
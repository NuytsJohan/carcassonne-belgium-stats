"""Import BCOC 2026 match data into tournament_matches table.
Reconstructs match data primarily from BGA game records.
"""
import duckdb
from datetime import date

DB_PATH = "data/carcassonne.duckdb"
TOURNAMENT_ID = 14
MAX_GAP_SECONDS = 30 * 60

# Group compositions (from bracket images on carcassonnebelgium.weebly.com)
GROUPS = {
    "Group A": {
        "players": [
            (18, "jorenderidder"), (466, "Baesje"), (10, "Nicedicer"),
            (138, "alex988"), (7, "71Knives"), (4, "Bangla"),
            (544, "Bombero47"), (16, "Zwonnie"), (48, "PascalWe"),
        ],
        "date_start": "2025-11-25",
        "date_end": "2026-01-01",
    },
    "Group B": {
        "players": [
            (8, "Learn to fly"), (44, "CraftyRaf"), (23, "Defdamesdompi"),
            (21, "N2xU"), (288, "kestman82"), (15, "rally8"),
            (304, "Tom Ja"), (328, "Bagge0"), (9, "JinaJina"),
        ],
        "date_start": "2025-11-25",
        "date_end": "2026-01-01",
    },
    "Group C": {
        "players": [
            (22, "Carcharoth 9"), (11, "Saodadje"), (3, "King_Jo"),
            (3528, "Death Meeple"), (24, "Dextyle"), (550, "KD_12"),
            (25, "speler nico"),
        ],
        "date_start": "2025-11-25",
        "date_end": "2026-01-01",
        "dsq": ["arinus", "andreamea"],
    },
    "Group D": {
        "players": [
            (14, "mobidic"), (6, "ludojahhjahh"), (17, "valmir79"),
            (33, "obiwonder"), (38, "FabianM_be"), (3751, "Nubro"),
            (40, "Creaviasyl"), (26, "Patman-Duplo"),
        ],
        "date_start": "2025-11-25",
        "date_end": "2026-01-01",
        "dsq": ["JSM89"],
    },
}

# Knockout matches (from bracket image)
KNOCKOUT = [
    ("Round of 12", 81, "Saodadje", 11, "CraftyRaf", 44, 0, 2, None),
    ("Round of 12", 82, "ludojahhjahh", 6, "Nicedicer", 10, 1, 2, None),
    ("Round of 12", 83, "Baesje", 466, "valmir79", 17, 1, 2, None),
    ("Round of 12", 84, "JinaJina", 9, "King_Jo", 3, 2, 0, None),
    ("1/4 Finals", 41, "jorenderidder", 18, "CraftyRaf", 44, 0, 2, None),
    ("1/4 Finals", 42, "Learn to fly", 8, "Nicedicer", 10, 2, 0, None),
    ("1/4 Finals", 43, "Carcharoth 9", 22, "valmir79", 17, 0, 2, None),
    ("1/4 Finals", 44, "mobidic", 14, "JinaJina", 9, 2, 0, None),
    ("1/2 Finals", 21, "CraftyRaf", 44, "Learn to fly", 8, 2, 0, None),
    ("1/2 Finals", 22, "valmir79", 17, "mobidic", 14, 0, 2, None),
    ("3rd Place", None, "Learn to fly", 8, "valmir79", 17, 1, 2, None),
    ("Final", None, "CraftyRaf", 44, "mobidic", 14, 2, 0, None),
]


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


def find_match_cluster(con, p1_id, p2_id, date_start, date_end):
    """Find game clusters between two players in a date range."""
    games = con.execute("""
        SELECT g.id, g.played_at, gp1.rank as r1, gp2.rank as r2
        FROM games g
        JOIN game_players gp1 ON g.id = gp1.game_id AND gp1.player_id = ?
        JOIN game_players gp2 ON g.id = gp2.game_id AND gp2.player_id = ?
        WHERE g.played_at BETWEEN ? AND ?
        ORDER BY g.played_at
    """, [p1_id, p2_id, date_start, date_end]).fetchall()

    clusters = []
    current = []
    for g in games:
        if current and (g[1] - current[-1][1]).total_seconds() > MAX_GAP_SECONDS:
            clusters.append(current)
            current = []
        current.append(g)
    if current:
        clusters.append(current)

    return [c for c in clusters if len(c) >= 2]


def determine_result(s1, s2):
    if s1 > s2:
        return "1"
    elif s2 > s1:
        return "2"
    return "D"


def main():
    con = duckdb.connect(DB_PATH)

    # Create tournament
    con.execute("""
        INSERT INTO tournaments (id, name, type, year, edition, date_start, date_end, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [TOURNAMENT_ID, "BCOC 2026", "BCOC", 2026, "2026",
          "2025-12-01", "2026-02-01",
          "Belgian Championship of Carcassonne Online 2026. Groups Dec 1-30 2025, Knockouts Jan 1 - Feb 1 2026."])
    print("Tournament BCOC 2026 created (id=14).")

    # Create new players if needed
    new_player_ids = {}
    for name in ["arinus"]:
        existing = con.execute("SELECT id FROM players WHERE LOWER(name) = LOWER(?)", [name]).fetchone()
        if existing:
            new_player_ids[name] = existing[0]
        else:
            pid = con.execute(
                "INSERT INTO players (name, country) VALUES (?, 'BE') RETURNING id", [name]
            ).fetchone()[0]
            new_player_ids[name] = pid
            print(f"Created player '{name}' (id={pid}).")

    # ── Group stage: reconstruct from BGA data ───────────────────────────────

    total_inserted = 0
    total_with_games = 0

    for grp_name, grp_data in GROUPS.items():
        players = list(grp_data["players"])
        ds = grp_data["date_start"]
        de = grp_data["date_end"]

        match_num = 0
        grp_found = 0

        # For each pair, find BGA match
        for i in range(len(players)):
            for j in range(i + 1, len(players)):
                p1_id, p1_name = players[i]
                p2_id, p2_name = players[j]
                match_num += 1

                clusters = find_match_cluster(con, p1_id, p2_id, ds, de)

                if len(clusters) == 1:
                    match_games, s1, s2 = simulate_bo3(clusters[0])
                    gids = match_games + [None] * (3 - len(match_games))
                    result = determine_result(s1, s2)

                    if s1 == s2:
                        con.execute("""
                            INSERT INTO tournament_matches
                                (tournament_id, stage, match_number, player_1_id, player_2_id,
                                 score_1, score_2, result, game_id_1, game_id_2, notes)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'D', ?, ?, 'incomplete')
                        """, [TOURNAMENT_ID, grp_name, match_num, p1_id, p2_id,
                              s1, s2, gids[0], gids[1]])
                    else:
                        con.execute("""
                            INSERT INTO tournament_matches
                                (tournament_id, stage, match_number, player_1_id, player_2_id,
                                 score_1, score_2, result, game_id_1, game_id_2, game_id_3)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [TOURNAMENT_ID, grp_name, match_num, p1_id, p2_id,
                              s1, s2, result, gids[0], gids[1], gids[2]])

                    total_with_games += 1
                elif len(clusters) == 0:
                    con.execute("""
                        INSERT INTO tournament_matches
                            (tournament_id, stage, match_number, player_1_id, player_2_id,
                             score_1, score_2, result, notes)
                        VALUES (?, ?, ?, ?, ?, 0, 0, 'D', 'no games found')
                    """, [TOURNAMENT_ID, grp_name, match_num, p1_id, p2_id])
                else:
                    # Ambiguous - take the one in group stage date range
                    group_clusters = [c for c in clusters
                                      if date(2025, 12, 1) <= c[0][1].date() <= date(2025, 12, 30)]
                    if len(group_clusters) == 1:
                        match_games, s1, s2 = simulate_bo3(group_clusters[0])
                        gids = match_games + [None] * (3 - len(match_games))
                        result = determine_result(s1, s2)
                        con.execute("""
                            INSERT INTO tournament_matches
                                (tournament_id, stage, match_number, player_1_id, player_2_id,
                                 score_1, score_2, result, game_id_1, game_id_2, game_id_3)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [TOURNAMENT_ID, grp_name, match_num, p1_id, p2_id,
                              s1, s2, result, gids[0], gids[1], gids[2]])
                        total_with_games += 1
                    else:
                        con.execute("""
                            INSERT INTO tournament_matches
                                (tournament_id, stage, match_number, player_1_id, player_2_id,
                                 score_1, score_2, result, notes)
                            VALUES (?, ?, ?, ?, ?, 0, 0, 'D', 'ambiguous')
                        """, [TOURNAMENT_ID, grp_name, match_num, p1_id, p2_id])

                total_inserted += 1
                grp_found += 1

        # DSQ matches
        for dsq_name in grp_data.get("dsq", []):
            dsq_id = new_player_ids.get(dsq_name)
            if not dsq_id:
                dsq_id = con.execute("SELECT id FROM players WHERE LOWER(name) = LOWER(?)",
                                     [dsq_name]).fetchone()
                if dsq_id:
                    dsq_id = dsq_id[0]
                else:
                    print(f"WARNING: DSQ player '{dsq_name}' not found!")
                    continue

            for pid, pname in players:
                match_num += 1
                con.execute("""
                    INSERT INTO tournament_matches
                        (tournament_id, stage, match_number, player_1_id, player_2_id,
                         score_1, score_2, result, notes)
                    VALUES (?, ?, ?, ?, ?, 0, 0, 'D', 'dsq')
                """, [TOURNAMENT_ID, grp_name, match_num, pid, dsq_id])
                total_inserted += 1

        # DSQ vs DSQ (if multiple DSQ players in same group)
        dsq_names = grp_data.get("dsq", [])
        if len(dsq_names) > 1:
            dsq_ids = []
            for dn in dsq_names:
                did = new_player_ids.get(dn)
                if not did:
                    row = con.execute("SELECT id FROM players WHERE LOWER(name) = LOWER(?)", [dn]).fetchone()
                    if row:
                        did = row[0]
                if did:
                    dsq_ids.append(did)
            for i in range(len(dsq_ids)):
                for j in range(i + 1, len(dsq_ids)):
                    match_num += 1
                    con.execute("""
                        INSERT INTO tournament_matches
                            (tournament_id, stage, match_number, player_1_id, player_2_id,
                             score_1, score_2, result, notes)
                        VALUES (?, ?, ?, ?, ?, 0, 0, 'D', 'dsq')
                    """, [TOURNAMENT_ID, grp_name, match_num, dsq_ids[i], dsq_ids[j]])
                    total_inserted += 1

        print(f"{grp_name}: {grp_found} matches ({match_num} total incl DSQ)")

    # ── Knockout matches ─────────────────────────────────────────────────────

    KO_START = "2025-12-28"
    KO_END = "2026-02-10"

    ko_matched = 0
    for stage, mnum, p1_name, p1_id, p2_name, p2_id, s1, s2, notes in KNOCKOUT:
        result = determine_result(s1, s2)

        # Try to find BGA games
        clusters = find_match_cluster(con, p1_id, p2_id, KO_START, KO_END)

        # Filter to knockout period
        ko_clusters = [c for c in clusters
                       if date(2026, 1, 1) <= c[0][1].date() <= date(2026, 2, 1)]

        gids = [None, None, None]
        if len(ko_clusters) == 1:
            match_games, ns1, ns2 = simulate_bo3(ko_clusters[0])
            gids = match_games + [None] * (3 - len(match_games))
            if ns1 != s1 or ns2 != s2:
                print(f"KO SCORE FIX: {p1_name} vs {p2_name} ({stage}): {s1}-{s2} -> {ns1}-{ns2}")
                s1, s2 = ns1, ns2
                result = determine_result(s1, s2)
            ko_matched += 1

        con.execute("""
            INSERT INTO tournament_matches
                (tournament_id, stage, match_number, player_1_id, player_2_id,
                 score_1, score_2, result, notes, game_id_1, game_id_2, game_id_3)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [TOURNAMENT_ID, stage, mnum, p1_id, p2_id, s1, s2, result, notes,
              gids[0], gids[1], gids[2]])
        total_inserted += 1

    print(f"\nKnockout: {len(KNOCKOUT)} matches ({ko_matched} with BGA games)")

    # ── Summary ──────────────────────────────────────────────────────────────

    total = con.execute("SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?",
                        [TOURNAMENT_ID]).fetchone()[0]
    with_games = con.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND game_id_1 IS NOT NULL",
        [TOURNAMENT_ID]).fetchone()[0]
    no_games = con.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND notes = 'no games found'",
        [TOURNAMENT_ID]).fetchone()[0]
    dsq_count = con.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND notes = 'dsq'",
        [TOURNAMENT_ID]).fetchone()[0]

    print(f"\nTotal: {total} matches")
    print(f"With BGA games: {with_games}")
    print(f"No games found: {no_games}")
    print(f"DSQ: {dsq_count}")

    rows = con.execute("""
        SELECT stage, COUNT(*) FROM tournament_matches
        WHERE tournament_id = ? GROUP BY stage ORDER BY stage
    """, [TOURNAMENT_ID]).fetchall()
    for stage, cnt in rows:
        print(f"  {stage}: {cnt}")

    con.close()


if __name__ == "__main__":
    main()
"""BCLC 2025 - Swiss rounds 1-6 (games only, no tournament_matches)."""
import re
from pathlib import Path

import duckdb
import openpyxl

ROOT = Path(__file__).parents[1]
DB_PATH = ROOT / "data" / "carcassonne.duckdb"
XLSX = ROOT / "data" / "raw" / "bk" / "BK Carcassonne 2025.xlsx"

TID = 31
DATE = "2025-01-01"

NAME_OVERRIDES = {
    "John Vanhees": "John Van Hees",
    "Lorenzo Van herrewege": "Lorenzo Van Herrewege",
    "Guy Cornelis": "Guy Comelis",
    "Johan Van der Wal": "Johan Van Der Wal",
    "Tim Ongena": "Tim Onghena",
    "Karolien Thys": "Karolien Thijs",
}

TABLE_RE = re.compile(r"Carcassonne,\s*Ronde\s*(\d+),\s*Tafel\s*(\d+)")


def pid(conn, name_nl: str) -> int:
    canonical = NAME_OVERRIDES.get(name_nl, name_nl)
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [canonical]
    ).fetchone()
    assert row, f"Missing player: {name_nl!r} (canonical {canonical!r})"
    return row[0]


def parse_round(ws):
    """Yield (round_num, table_num, [(first, last, rank, score), (first, last, rank, score)])."""
    rows = list(ws.iter_rows(values_only=True))
    i = 0
    while i < len(rows):
        row = rows[i]
        if row[0] == "Spel/Jeu : " and isinstance(row[1], str):
            m = TABLE_RE.search(row[1])
            if m:
                rnum = int(m.group(1))
                tnum = int(m.group(2))
                p1 = rows[i + 2]
                p2 = rows[i + 3]
                players = []
                for pr in (p1, p2):
                    first = pr[2].strip() if isinstance(pr[2], str) else pr[2]
                    last = pr[3].strip() if isinstance(pr[3], str) else pr[3]
                    rank = int(pr[4]) if pr[4] is not None else None
                    score = int(pr[5]) if pr[5] is not None else None
                    players.append((first, last, rank, score))
                yield rnum, tnum, players
                i += 7
                continue
        i += 1


def main():
    conn = duckdb.connect(str(DB_PATH))

    # Check no existing Swiss games (round 1-6) for this tournament
    existing = conn.execute(
        "SELECT COUNT(*) FROM games WHERE tournament_id=? AND round BETWEEN 1 AND 6 AND source='swiss'",
        [TID],
    ).fetchone()[0]
    if existing:
        print(f"Already {existing} swiss games for tid={TID}; aborting.")
        conn.close()
        return

    wb = openpyxl.load_workbook(XLSX, data_only=True)

    total_games = 0
    for sn in ["Ronde 1", "Ronde 2", "Ronde 3", "Ronde 4", "Ronde 5", "Ronde 6"]:
        ws = wb[sn]
        round_games = 0
        for rnum, tnum, players in parse_round(ws):
            (f1, l1, r1, s1), (f2, l2, r2, s2) = players
            n1 = f"{f1} {l1}"
            n2 = f"{f2} {l2}"
            a_pid = pid(conn, n1)
            b_pid = pid(conn, n2)

            gid = conn.execute(
                """
                INSERT INTO games (tournament_id, round, table_number, source, played_at)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
                """,
                [TID, rnum, tnum, "swiss", DATE],
            ).fetchone()[0]

            conn.execute(
                "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
                [gid, a_pid, s1, r1],
            )
            conn.execute(
                "INSERT INTO game_players (game_id, player_id, score, rank) VALUES (?, ?, ?, ?)",
                [gid, b_pid, s2, r2],
            )
            round_games += 1
        print(f"{sn}: {round_games} games")
        total_games += round_games

    print(f"\nTotal Swiss games imported: {total_games}")
    conn.close()


if __name__ == "__main__":
    main()

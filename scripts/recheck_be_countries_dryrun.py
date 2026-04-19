"""Dry-run: re-fetch BGA country for every player currently marked country='BE'
with a BGA player ID. Reports discrepancies; does NOT update the database.

Uses the saved BGA session in data/bga_session (run scripts/bga_save_session.py
first if it doesn't exist). No email/password needed.
"""
import asyncio
import csv
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parents[1]))

from src.importers.bga_fetcher import fetch_player_country, get_token_and_cookies

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
REPORT_PATH = Path(__file__).parents[1] / "data" / "be_country_recheck.csv"


def _run_async(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    return loop.run_until_complete(coro)


def main():
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    players = conn.execute("""
        SELECT id, name, bga_player_id
        FROM players
        WHERE country = 'BE' AND bga_player_id IS NOT NULL
        ORDER BY id
    """).fetchall()
    conn.close()

    print(f"{len(players)} BE-with-BGA players to re-check.")
    if not players:
        return

    print("Obtaining BGA token via saved session ...")
    first_pid = int(players[0][2])
    token, cookies = _run_async(
        get_token_and_cookies(email="", password="", player_id=first_pid, headless=True)
    )
    print("Token OK. Starting scan.\n")

    rows = []
    non_be = 0
    not_found = 0
    for idx, (internal_id, name, bga_pid) in enumerate(players, 1):
        fetched = fetch_player_country(int(bga_pid), token, cookies)
        if fetched is None:
            status = "not_found"
            not_found += 1
        elif fetched == "BE":
            status = "ok"
        else:
            status = "MISMATCH"
            non_be += 1

        rows.append({
            "internal_id": internal_id,
            "name": name,
            "bga_player_id": bga_pid,
            "current_country": "BE",
            "bga_country": fetched or "",
            "status": status,
        })

        if status != "ok":
            print(f"  [{idx:>4}/{len(players)}] {name} (BGA {bga_pid}) "
                  f"-> {fetched or 'None'}  [{status}]")

        # periodic progress tick
        if idx % 50 == 0:
            print(f"  ... progress: {idx}/{len(players)} "
                  f"(mismatches={non_be}, not_found={not_found})")

    print(f"\nScan complete: {len(players)} players | "
          f"{non_be} non-BE | {not_found} not found | "
          f"{len(players) - non_be - not_found} confirmed BE")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()

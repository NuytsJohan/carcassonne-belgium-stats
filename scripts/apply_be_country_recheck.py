"""Apply the 45 country corrections from data/be_country_recheck.csv.

Only rows with status='MISMATCH' are applied. 'not_found' rows are left as
country='BE' (we can't verify, so don't touch).
"""
import csv
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
REPORT = Path(__file__).parents[1] / "data" / "be_country_recheck.csv"


def main():
    rows = []
    with REPORT.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "MISMATCH":
                rows.append(r)

    print(f"{len(rows)} mismatches to apply.")
    if not rows:
        return

    conn = duckdb.connect(str(DB_PATH))
    updated = 0
    for r in rows:
        pid = int(r["internal_id"])
        new_country = r["bga_country"].strip()
        if not new_country:
            print(f"  skip {r['name']}: empty bga_country")
            continue
        # Safety: only update rows still at 'BE'
        res = conn.execute(
            "UPDATE players SET country = ? WHERE id = ? AND country = 'BE'",
            [new_country, pid],
        )
        changed = res.fetchone()[0]
        if changed:
            print(f"  {r['name']} (id={pid}): BE -> {new_country}")
            updated += 1
        else:
            print(f"  {r['name']} (id={pid}): skipped (no longer BE)")
    conn.close()
    print(f"\nDone. {updated}/{len(rows)} rows updated.")


if __name__ == "__main__":
    main()

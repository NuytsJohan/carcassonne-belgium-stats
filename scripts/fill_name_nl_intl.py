"""Fill players.name_nl for international players matched by BGA handle."""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

ENTRIES = [
    ("Navarre79", "Ralph Querfurth"),
    ("Lammkeule", "Sebastian Trunz"),
    ("Humske", "Els Bulten"),
    ("Moya88", "Martin Mojzis"),
    ("pinder_tron", "Takafumi Mochizuki"),
    ("Someone_you_know", "Vladimir Kovalev"),
    ("UY_Scuti", "Tomasz Preuss"),
    ("progressive_topline", "Genro Fujimoto"),
    ("Mars15", "Marian Curcan"),
    ("TerranPL", "Maciej Polak"),
    ("g3rappa", "Arpad Gere"),
    ("Statmatt", "Matt Tucker"),
    ("Senglar", "Daniel Angelats"),
    ("qazxswedc", "Xiangyu Qin"),
    ("Alexey_LV", "Aleksejs Pegusevs"),
    ("HotlyHotly", "Min-Wei Chen"),
    ("Szigfrid", "Jozsef Tihon"),
    ("The Alchimist", "Horacio Mastandrea"),
    ("adrear", "Kyrylo Manakhov"),
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    matched = 0
    unmatched = []
    ambiguous = []

    for bga_name, real_name in ENTRIES:
        rows = conn.execute(
            "SELECT id, country FROM players WHERE LOWER(name) = LOWER(?)",
            [bga_name],
        ).fetchall()
        if not rows:
            unmatched.append((bga_name, real_name))
            continue
        if len(rows) > 1:
            ambiguous.append((bga_name, real_name, rows))
            continue
        conn.execute(
            "UPDATE players SET name_nl = ? WHERE id = ?",
            [real_name, rows[0][0]],
        )
        matched += 1

    print(f"Matched/updated: {matched} / {len(ENTRIES)}")
    if unmatched:
        print("\n--- Unmatched ---")
        for bga, rn in unmatched:
            print(f"  {bga} -> {rn}")
    if ambiguous:
        print("\n--- Ambiguous ---")
        for bga, rn, rows in ambiguous:
            print(f"  {bga} -> {rn}: {rows}")

    conn.close()


if __name__ == "__main__":
    main()

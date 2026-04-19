"""Renumber WTCOC/ETCOC duel stages 'Match N' sequentially per tournament,
ordered by date_played (oldest first), ties broken by id.

Friendlies are already numbered correctly. Non-'Match N' stages
(e.g. 'Gr.Stage 1', 'Quarter-final') are left untouched.
"""
import re
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"
MATCH_RE = re.compile(r"^Match\s+\d+$")


def main():
    conn = duckdb.connect(str(DB_PATH))

    tournaments = conn.execute(
        """
        SELECT id, name
        FROM tournaments
        WHERE type IN ('WTCOC', 'ETCOC')
        ORDER BY year, id
        """
    ).fetchall()

    # DuckDB raises an FK error on any UPDATE of a row referenced by another
    # table (even when the FK column isn't modified). Workaround: snapshot
    # nations_matches, delete them, update the duel stages, re-insert matches.
    match_cols = [
        "id", "duel_id", "player_id", "opponent_player_id",
        "score_belgium", "score_opponent", "result",
        "game_id_1", "game_id_2", "game_id_3", "game_id_4", "game_id_5",
        "notes",
    ]
    col_list = ", ".join(match_cols)
    placeholders = ", ".join(["?"] * len(match_cols))

    matches_snapshot = conn.execute(
        f"SELECT {col_list} FROM nations_matches"
    ).fetchall()
    print(f"Snapshotted {len(matches_snapshot)} nations_matches rows.")

    conn.execute("DELETE FROM nations_matches")
    conn.execute("CHECKPOINT")

    total_updates = 0
    for t_id, t_name in tournaments:
        duels = conn.execute(
            """
            SELECT id, stage, date_played, opponent_country
            FROM nations_competition_duels
            WHERE tournament_id = ?
            ORDER BY date_played NULLS LAST, id
            """,
            [t_id],
        ).fetchall()

        match_duels = [d for d in duels if d[1] and MATCH_RE.match(d[1])]
        if not match_duels:
            continue

        print(f"\n[{t_id}] {t_name}: {len(match_duels)} Match duels")

        for duel_id, _, _, _ in match_duels:
            conn.execute(
                "UPDATE nations_competition_duels SET stage = ? WHERE id = ?",
                [f"__TMP_{duel_id}", duel_id],
            )

        for idx, (duel_id, old_stage, date_played, opp) in enumerate(match_duels, 1):
            new_stage = f"Match {idx}"
            conn.execute(
                "UPDATE nations_competition_duels SET stage = ? WHERE id = ?",
                [new_stage, duel_id],
            )
            if old_stage != new_stage:
                print(f"  duel {duel_id:>3} ({date_played} vs {opp}): "
                      f"{old_stage!r} -> {new_stage!r}")
                total_updates += 1
            else:
                print(f"  duel {duel_id:>3} ({date_played} vs {opp}): "
                      f"{new_stage!r} (unchanged)")

    for row in matches_snapshot:
        conn.execute(
            f"INSERT INTO nations_matches ({col_list}) VALUES ({placeholders})",
            list(row),
        )
    restored = conn.execute("SELECT COUNT(*) FROM nations_matches").fetchone()[0]
    assert restored == len(matches_snapshot), (
        f"restored {restored} != snapshot {len(matches_snapshot)}"
    )
    print(f"\nRestored {restored} nations_matches rows.")

    conn.close()
    print(f"\nDone. {total_updates} stages renumbered.")


if __name__ == "__main__":
    main()

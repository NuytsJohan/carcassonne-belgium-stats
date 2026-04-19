"""Import BCL 2006 (Belgian Live Championship) final ranking.

Source: BK Carcassonne 2006finale.pdf
Names in PDF are mostly 'Surname Firstname'; exceptions (already first-first):
Krzystof Detemmerman, Inge De Herdt.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TOURNAMENT_ID = 15
TOURNAMENT_NAME = "BCLC 2006"
TOURNAMENT_TYPE = "BCLC"
TOURNAMENT_YEAR = 2006

# (final_rank, name_nl, win_pct, total_score)
# name_nl is normalised to 'Firstname Surname'.
PARTICIPANTS = [
    (1,  "Johan Nuyts",          0.9058, 225),
    (2,  "Danny Coenen",         0.8943, 251),
    (3,  "Frederik De Lie",      0.8936, 225),
    (4,  "Bert De Norre",        0.8584, 213),
    (5,  "Dominic Reberez",      0.8338, 204),
    (6,  "Wesley Stroop",        0.8094, 232),
    (7,  "Benny Verbinnen",      0.7653, 184),
    (8,  "Krzystof Detemmerman", 0.7549, 213),
    (9,  "Peter Magchiels",      0.7443, 198),
    (10, "Marleen Polfliet",     0.7424, 188),
    (11, "Pieter Iserbyt",       0.7167, 192),
    (12, "Nico De Bleye",        0.7096, 172),
    (13, "Lieven Piqueur",       0.6901, 195),
    (14, "Inge De Herdt",        0.6819, 189),
    (15, "Davy Baetens",         0.6794, 170),
    (16, "Tom Van Lier",         0.6564, 171),
    (17, "David Schraepen",      0.6546, 183),
    (18, "Roel Van Doorsselaer", 0.6458, 165),
    (19, "Birgen Boons",         0.6412, 168),
    (20, "Bart Devreker",        None,   None),
]


def find_or_create_player(conn, name_nl: str) -> tuple[int, bool]:
    """Return (player_id, created). Matches existing name_nl case-insensitively."""
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)",
        [name_nl],
    ).fetchone()
    if row:
        return row[0], False
    # Insert new player: name and name_nl both set to real name.
    new_id = conn.execute(
        "INSERT INTO players (name, name_nl, country) VALUES (?, ?, 'BE') RETURNING id",
        [name_nl, name_nl],
    ).fetchone()[0]
    return new_id, True


def main():
    conn = duckdb.connect(str(DB_PATH))

    existing = conn.execute(
        "SELECT id FROM tournaments WHERE id = ?", [TOURNAMENT_ID]
    ).fetchone()
    if existing:
        print(f"Tournament id={TOURNAMENT_ID} already exists; aborting.")
        conn.close()
        return

    conn.execute(
        """
        INSERT INTO tournaments (id, name, type, year, edition, date_start, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            TOURNAMENT_ID,
            TOURNAMENT_NAME,
            TOURNAMENT_TYPE,
            TOURNAMENT_YEAR,
            "2006",
            "2006-01-01",
            "Belgian Live Championship 2006 - final ranking from BK Carcassonne 2006finale.pdf",
        ],
    )
    print(f"Created tournament {TOURNAMENT_NAME} (id={TOURNAMENT_ID}).")

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created_players = []
    matched_players = []
    for rank, name_nl, win_pct, total_score in PARTICIPANTS:
        pid, created = find_or_create_player(conn, name_nl)
        (created_players if created else matched_players).append((pid, name_nl))

        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, total_score, win_pct)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TOURNAMENT_ID, pid, rank, total_score, win_pct],
        )
        next_tp_id += 1

    print(f"\nMatched existing players ({len(matched_players)}):")
    for pid, n in matched_players:
        print(f"  [{pid}] {n}")
    print(f"\nCreated new players ({len(created_players)}):")
    for pid, n in created_players:
        print(f"  [{pid}] {n}")

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?",
        [TOURNAMENT_ID],
    ).fetchone()[0]
    print(f"\nTotal participants for {TOURNAMENT_NAME}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

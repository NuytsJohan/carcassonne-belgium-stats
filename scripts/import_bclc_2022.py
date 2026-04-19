"""Import BCLC 2022 final ranking + top-16 playoff.

Source: two screenshots.
- Eindklassement after 6 rounds: 45 participants with "Tornooi" points (can be
  half-integers) and "Weerst" resistance (different scale from prior years; >1).
- Top 16 played a playoff bracket; final playoff rank shown in second image.

We store playoff final rank for ranks 1-16; Eindklassement rank for 17-45.
Pnt/Res stored for all 45.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

TID = 28
TNAME = "BCLC 2022"
TTYPE = "BCLC"
TYEAR = 2022

NAME_OVERRIDES = {
    "Bjorn Petit": "Bj\u00f6rn Petit",
    "Hans Gijde": "Hans Gijd\u00e9",
    "John Vanhees": "John Van Hees",
    "Ellen Hacquaert": "Ellen Hackaert",
    "Guy Cornelis": "Guy Comelis",
    "Tim Ongena": "Tim Onghena",  # assumed same person as 2019 'Onghena'; verify
}

# Eindklassement order -> (name_nl, points, resistance)
EINDKLASSEMENT = [
    ("Nicolas Victor",         34,   0.851988),
    ("Dominic R\u00e9b\u00e9rez", 33, 0.876163),
    ("Raf Mesotten",           33,   0.87413),
    ("Nico Wellemans",         32,   0.880873),
    ("Kim Peeters",            32,   0.845571),
    ("Philippe Kenens",        32,   0.747519),
    ("Nele De Pooter",         31,   0.862086),
    ("Els Smekens",            31,   0.843347),
    ("Niels Ongena",           31,   0.812151),
    ("Celien Neven",           30,   0.804674),
    ("Bert Jacobs",            30,   0.80452),
    ("Tim Ongena",             30,   0.790322),   # -> Tim Onghena
    ("Joren De Ridder",        30,   0.784626),
    ("Bjorn Petit",            30,   0.777173),   # -> Björn Petit
    ("Geert Wouters",          29,   0.806524),
    ("Patrick De Wilde",       29,   0.778054),
    ("Hans Gijde",             17,   1.260149),   # -> Hans Gijdé
    ("Michiel Beerden",        17,   1.238458),
    ("John Vanhees",           16,   1.355689),   # -> John Van Hees
    ("Maud Quaniers",          16,   1.335958),
    ("Karl Verheyden",         16,   1.335675),
    ("Ellen Hacquaert",        15,   1.268227),   # -> Ellen Hackaert
    ("Guy Cornelis",           14.5, 1.31594),    # -> Guy Comelis
    ("Wannes Vansina",         14.5, 1.259445),
    ("Johan Nuyts",            14,   1.304384),
    ("Rudy De Weerdt",         14,   1.243797),
    ("Vital Pluymers",         14,   1.239656),
    ("Antje Van den Heuvel",   13.5, 1.23826),
    ("Seijke Weeghmans",       13,   1.266142),
    ("Lieve Peirtsegaele",     13,   1.194947),
    ("Wolf Nuyts",             13,   1.173356),
    ("Marissa Sorgeloos",      12,   1.163101),
    ("Marc Peeters",           12,   1.144738),
    ("Tom De Smedt",           12,   1.25535),
    ("An Van Der Goten",       12,   1.208639),
    ("Arnaud De Vuyst",        12,   1.184202),
    ("Rita De Vos",            12,   1.184065),
    ("Jeremy Verbeke",         11.5, 1.194458),
    ("Grim Ongena",            11,   1.199641),
    ("Stephen Fleerackers",    11,   1.133639),
    ("Elise Robberechts",      11,   1.130928),
    ("Johanna Dumon",          11,   1.110362),
    ("Johan Van Der Wal",      11,   1.043895),
    ("An Delhaye",             10,   1.181927),
    ("Erik Van de Perre",       9,   0.945553),
]

# Playoff final ranks 1..16 (name_nl matches Eindklassement name spelling).
PLAYOFF_RANKS = [
    "Nicolas Victor",     # 1
    "Raf Mesotten",       # 2
    "Dominic R\u00e9b\u00e9rez",  # 3
    "Joren De Ridder",    # 4
    "Bert Jacobs",        # 5
    "Tim Ongena",         # 6
    "Els Smekens",        # 7
    "Celien Neven",       # 8
    "Nele De Pooter",     # 9
    "Niels Ongena",       # 10
    "Nico Wellemans",     # 11
    "Bjorn Petit",        # 12
    "Kim Peeters",        # 13
    "Philippe Kenens",    # 14
    "Patrick De Wilde",   # 15
    "Geert Wouters",      # 16
]


def find_or_create_player(conn, name_nl: str) -> tuple[int, bool]:
    canonical = NAME_OVERRIDES.get(name_nl, name_nl)
    row = conn.execute(
        "SELECT id FROM players WHERE LOWER(name_nl) = LOWER(?)", [canonical]
    ).fetchone()
    if row:
        return row[0], False
    new_id = conn.execute(
        "INSERT INTO players (name, name_nl, country) VALUES (?, ?, 'BE') RETURNING id",
        [canonical, canonical],
    ).fetchone()[0]
    return new_id, True


def main():
    conn = duckdb.connect(str(DB_PATH))

    if conn.execute("SELECT id FROM tournaments WHERE id = ?", [TID]).fetchone():
        print(f"Tournament id={TID} already exists; aborting.")
        conn.close()
        return

    conn.execute(
        """
        INSERT INTO tournaments (id, name, type, year, edition, date_start, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [TID, TNAME, TTYPE, TYEAR, "2022", "2022-01-01",
         "Belgian Live Championship 2022 - 45 participants; top 16 played "
         "playoff (final rank used for 1-16, Eindklassement rank for 17-45). "
         "Pnt/Res use 2022 scoring (different scale from prior editions)."],
    )
    print(f"Created {TNAME} (id={TID}).")

    # Build rank map: playoff rank for top-16 else Eindklassement rank.
    playoff_rank_by_name = {n: i + 1 for i, n in enumerate(PLAYOFF_RANKS)}

    max_tp = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tournament_participants").fetchone()[0]
    next_tp_id = max_tp + 1

    created, matched = [], []
    for eind_rank_idx, (name_nl, pts, res) in enumerate(EINDKLASSEMENT, start=1):
        pid, was_created = find_or_create_player(conn, name_nl)
        canonical = NAME_OVERRIDES.get(name_nl, name_nl)
        (created if was_created else matched).append((pid, canonical))

        final_rank = playoff_rank_by_name.get(name_nl, eind_rank_idx)

        conn.execute(
            """
            INSERT INTO tournament_participants
                (id, tournament_id, player_id, final_rank, points, resistance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [next_tp_id, TID, pid, final_rank, pts, res],
        )
        next_tp_id += 1

    print(f"\nMatched existing: {len(matched)} | Created new: {len(created)}")
    for pid, n in created:
        print(f"  NEW [{pid}] {n}")

    cnt = conn.execute(
        "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?", [TID]
    ).fetchone()[0]
    print(f"\nParticipants: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

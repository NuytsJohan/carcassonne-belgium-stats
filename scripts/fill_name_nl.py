"""Fill players.name_nl for Belgian players, matched by BGA handle.

Source: screenshot provided by user (BCOC roster with real names in brackets).
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

ENTRIES = [
    ("JinaJina", "Nicolas Victor"),
    ("N2xU", "Nico Wellemans"),
    ("mobidic", "Maud Quaniers"),
    ("jorenderidder", "Joren De Ridder"),
    ("71Knives", "Karl Verheyden"),
    ("rally8", "Bert Jacobs"),
    ("Defdamesdompi", "Dominic Reberez"),
    ("arinius", "Yannick Haenebalcke"),
    ("Bangla", "Tony Reynolds"),
    ("jumumu", "Juliette Munot"),
    ("CraftyRaf", "Raf Mesotten"),
    ("andreamea", "Andrea Messana"),
    ("obiwonder", "Johan Nuyts"),
    ("Patman-Duplo", "Patrick Puttemans"),
    ("Nubro", "Bruno De Rooze"),
    ("wouterhuy", "Wouter Huylebroeck"),
    ("Wiewetda", "Nico Guldentops"),
    ("GER300", "Gérald Vanbellingen"),
    ("Nicedicer", "Tom De Smedt"),
    ("vanbaekel-", "Wannes Van Baekel"),
    ("JSM89", "Jeroen Smolders"),
    ("Edeloup", "Esther de Behr"),
    ("thejoker2", "Joke Stroobants"),
    ("PascalWe", "Pascal Wellemans"),
    ("avadego", "An Van Der Goten"),
    ("AnTHology", "Anthony Abbeloos"),
    ("aarcia", "Manu Van Haverbeke"),
    ("pumple81", "Clémentine Calvet"),
    ("Carcasas", "Saskia Huylebroeck"),
    ("Driestruction", "Dries Mertens"),
    ("Creaviasyl", "Sylvia Foerier"),
    ("Antje52", "Antje Van den Heuvel"),
    ("Carcharoth 9", "Wannes Vansina"),
    ("LasseVanAsse", "Lasse Reberez"),
    ("speler nico", "Nico Verlinden"),
    ("Rafalinow", "Rafael Gielis"),
    ("papinau", "Michel Heughebaert"),
    ("Sicarius Lupus", "Wolf Nuyts"),
    ("FabianM_be", "Fabian Mouton"),
    ("Zwonnie", "Ronny Korsten"),
    ("Wolf56", "Rudy De Weerdt"),
    ("cruantix", "Dries Decru"),
    ("Saodadje", "Simon Desset"),
    ("Stoony13", "Toon De Bock"),
    ("kestman82", "Kristof Kesteloot"),
    ("Boulette1205", "Christine Coulon"),
    ("ludojahhjahh", "Ludovic Jahnert"),
    ("dextyle", "Decio Denis Bernardo"),
    ("San Pedros", "Geert De Loenzien"),
    ("valmir79", "Thomas Declerck"),
    ("Baesje", "Servaes Hereman"),
    ("Death Meeple", "Nicolas Rousseaux"),
    ("zusjezus", "Mieke Suenens"),
    ("SirBuddey", "Andres Hennebel"),
    ("Amed3e", "Kristof Hennebel"),
    ("King_Jo", "Joachim Deschuytter"),
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    matched = 0
    unmatched = []
    ambiguous = []

    for bga_name, real_name in ENTRIES:
        rows = conn.execute(
            "SELECT id FROM players WHERE LOWER(name) = LOWER(?)",
            [bga_name],
        ).fetchall()
        if not rows:
            unmatched.append((bga_name, real_name))
            continue
        if len(rows) > 1:
            # Prefer Belgian player
            be_rows = conn.execute(
                "SELECT id FROM players WHERE LOWER(name) = LOWER(?) AND country = 'BE'",
                [bga_name],
            ).fetchall()
            if len(be_rows) == 1:
                rows = be_rows
            else:
                ambiguous.append((bga_name, real_name, [r[0] for r in rows]))
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
        for bga, rn, ids in ambiguous:
            print(f"  {bga} -> {rn}: ids={ids}")

    conn.close()


if __name__ == "__main__":
    main()
"""Flag WTCOC team players as national_team = TRUE.

Source: https://www.carcassonne.cat/wtcoc/teams.php
Matches by player name (case-insensitive). Unmatched names are printed.
"""
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

COUNTRY_MAP = {
    "Argentina": "AR", "Australia": "AU", "Belarus": "BY", "Belgium": "BE",
    "Brazil": "BR", "Catalonia": "ES", "Chile": "CL", "China": "CN",
    "Colombia": "CO", "Croatia": "HR", "Czech Republic": "CZ", "Ecuador": "EC",
    "Finland": "FI", "France": "FR", "Germany": "DE", "Greece": "GR",
    "Guatemala": "GT", "Hong Kong": "HK", "Hungary": "HU", "Italy": "IT",
    "Japan": "JP", "Latvia": "LV", "Lithuania": "LT", "Malaysia": "MY",
    "Mexico": "MX", "Netherlands": "NL", "Peru": "PE", "Poland": "PL",
    "Portugal": "PT", "RCP": "RU", "Romania": "RO", "Spain": "ES",
    "Sweden": "SE", "Taiwan": "TW", "Thailand": "TH", "Ukraine": "UA",
    "United Kingdom": "GB", "United States": "US", "Uruguay": "UY",
    "Vietnam": "VN",
}

ENTRIES = [
    ("Argentina", "Academia47"), ("Argentina", "Hellkkitte"), ("Argentina", "herchu"),
    ("Argentina", "maticarrizoc"), ("Argentina", "Maxcachomba"), ("Argentina", "maxi8830"),
    ("Argentina", "Nicoooo95"), ("Argentina", "Santiblader"), ("Argentina", "SirGiuli"),
    ("Argentina", "webbi"),
    ("Australia", "5murf"), ("Australia", "chilp"), ("Australia", "King_Deded4d5"),
    ("Australia", "Megot Noskill"), ("Australia", "narelle_s"), ("Australia", "robw335"),
    ("Australia", "Shayne"), ("Australia", "STE MUZZ 1"), ("Australia", "tj26"),
    ("Australia", "VelcroDog"),
    ("Belarus", "Gladki_Alex"), ("Belarus", "Ivaks"), ("Belarus", "Korshuk Dima"),
    ("Belarus", "Levitin"), ("Belarus", "NataSa23"), ("Belarus", "PabLit8"),
    ("Belarus", "PushkinBLR"), ("Belarus", "Ryner-kun"), ("Belarus", "vadzimir"),
    ("Belarus", "Viktore"),
    ("Belgium", "Bangla"), ("Belgium", "Carcharoth 9"), ("Belgium", "CraftyRaf"),
    ("Belgium", "Defdamesdompi"), ("Belgium", "JinaJina"), ("Belgium", "Learn to fly"),
    ("Belgium", "mobidic"), ("Belgium", "N2xU"), ("Belgium", "Nicedicer"),
    ("Belgium", "valmir79"),
    ("Brazil", "Berna1871"), ("Brazil", "Besson00"), ("Brazil", "Ed chamon"),
    ("Brazil", "FauBaRR"), ("Brazil", "gdarruda"), ("Brazil", "KR_Knight_Realm"),
    ("Brazil", "mejuto1408"), ("Brazil", "sir gus"), ("Brazil", "Zedson"),
    ("Catalonia", "Borberichi"), ("Catalonia", "Carquinyolis"), ("Catalonia", "Danisvh"),
    ("Catalonia", "Defdean"), ("Catalonia", "DrGalifardeuGalindoi"), ("Catalonia", "Erdeivit"),
    ("Catalonia", "Mademova"), ("Catalonia", "Niangoflow"), ("Catalonia", "senglar"),
    ("Catalonia", "Steam4206"),
    ("Chile", "adanlasheras"), ("Chile", "carito36"), ("Chile", "Claudio Jorquera"),
    ("Chile", "CrisRamirez"), ("Chile", "ht_bry"), ("Chile", "King Mauri"),
    ("Chile", "rbustamante23"), ("Chile", "Tito Arce"), ("Chile", "vainiria"),
    ("Chile", "vicsam01"),
    ("China", "203club"), ("China", "Aki_Stack"), ("China", "Dont Break My Heart"),
    ("China", "guguguleader"), ("China", "Kndr"), ("China", "mitaocat"),
    ("China", "qazxswedc"), ("China", "riddle771"), ("China", "superIRonHead"),
    ("China", "tomatoken"),
    ("Colombia", "dremkad"), ("Colombia", "eltalivan"), ("Colombia", "Ext4ur"),
    ("Colombia", "KILLuminati_Warrior"), ("Colombia", "maister35"), ("Colombia", "Niosorioc"),
    ("Colombia", "Pechitoz"), ("Colombia", "Salva3xz"), ("Colombia", "Tatan D"),
    ("Colombia", "TRojasHenao"),
    ("Croatia", "Bluste"), ("Croatia", "BuHa44"), ("Croatia", "El Tomek"),
    ("Croatia", "Lamprdlica"), ("Croatia", "Lilliam510"), ("Croatia", "lorki0503"),
    ("Croatia", "neddd365"), ("Croatia", "Tompa99"), ("Croatia", "vegaorion"),
    ("Croatia", "yuto171"),
    ("Czech Republic", "bormar"), ("Czech Republic", "chonps"), ("Czech Republic", "J0nny"),
    ("Czech Republic", "majkls"), ("Czech Republic", "martypartyouje"),
    ("Czech Republic", "Moya88"), ("Czech Republic", "posij118"),
    ("Czech Republic", "Simaster 1991"), ("Czech Republic", "smoula"),
    ("Czech Republic", "vomo_43"),
    ("Ecuador", "Danita91"), ("Ecuador", "Gary-san"), ("Ecuador", "JM2011"),
    ("Ecuador", "JorgEn309"), ("Ecuador", "Jotanan"), ("Ecuador", "JuankyEND"),
    ("Ecuador", "Pietroski"), ("Ecuador", "Sopadequinoa"), ("Ecuador", "tripeador22"),
    ("Ecuador", "Zavely"),
    ("Finland", "KajaaninOlli"), ("Finland", "Lapinkoski"), ("Finland", "R4muzero"),
    ("Finland", "Begonia maculata"), ("Finland", "GifusGray"), ("Finland", "Larpaattori"),
    ("Finland", "maelstrim"), ("Finland", "Nallerheim"), ("Finland", "Nethshrac"),
    ("Finland", "Tappajahaukii"),
    ("France", "Blitz"), ("France", "Boulym"), ("France", "Gat79Lux"),
    ("France", "houdini_Fr"), ("France", "Jadiema"), ("France", "Linkarssonne"),
    ("France", "Mano Negra"), ("France", "squallus"), ("France", "Valderic"),
    ("France", "viv-"),
    ("Germany", "amires21"), ("Germany", "HappyTim7"), ("Germany", "Karaboudjan"),
    ("Germany", "kostra"), ("Germany", "Leuschi"), ("Germany", "Maikitin"),
    ("Germany", "Meami"), ("Germany", "Mermadin"), ("Germany", "yzemaze"),
    ("Germany", "Zockerle"),
    ("Greece", "Asx3t0s"), ("Greece", "bs12"), ("Greece", "dark_passenger84"),
    ("Greece", "JohnMav"), ("Greece", "Loukasnks"), ("Greece", "mirr14"),
    ("Greece", "mpatis"), ("Greece", "Rico13mpatsoni"), ("Greece", "Ste-fa-na-kis"),
    ("Guatemala", "Chini_GT"), ("Guatemala", "ChrisWaight"), ("Guatemala", "D3xt3r99"),
    ("Guatemala", "dohkogt"), ("Guatemala", "Folkesterra"), ("Guatemala", "Kingoscar"),
    ("Guatemala", "Leykos"), ("Guatemala", "Paerci"), ("Guatemala", "Pj9416"),
    ("Guatemala", "Renebulsara"),
    ("Hong Kong", "CTCTCTCTCT"), ("Hong Kong", "Eugenetse98"), ("Hong Kong", "Helic"),
    ("Hong Kong", "Kingsley75"), ("Hong Kong", "Littlesmallsmall"), ("Hong Kong", "LSC"),
    ("Hong Kong", "panpan712"), ("Hong Kong", "Sunny369"), ("Hong Kong", "tabbybrown"),
    ("Hong Kong", "Waiwai1202"),
    ("Hungary", "coralos"), ("Hungary", "kissduska"), ("Hungary", "kissemo"),
    ("Hungary", "mosopal"), ("Hungary", "robill"), ("Hungary", "SilexSaxum"),
    ("Hungary", "szigfrid"), ("Hungary", "Tildi"), ("Hungary", "vallics"),
    ("Hungary", "vizecske"),
    ("Italy", "1984Anto"), ("Italy", "aibj7"), ("Italy", "Angostur4"),
    ("Italy", "barry10"), ("Italy", "giulioTm"), ("Italy", "James81"),
    ("Italy", "Lambe"), ("Italy", "Piccione85"), ("Italy", "pollo_verde"),
    ("Japan", "Dos_Diable_nero"), ("Japan", "f kenta"), ("Japan", "Kithara"),
    ("Japan", "marusingfire"), ("Japan", "nanatakkyu"), ("Japan", "Setsugentou"),
    ("Japan", "should"), ("Japan", "sou-tata"), ("Japan", "utime"),
    ("Japan", "YguruguruY"),
    ("Latvia", "Alexey_LV"), ("Latvia", "Alina 135"), ("Latvia", "AppleG"),
    ("Latvia", "Dirty old town"), ("Latvia", "krisna"), ("Latvia", "MrNumbers"),
    ("Latvia", "napalmEye"), ("Latvia", "Norberts96"), ("Latvia", "R2RIO"),
    ("Latvia", "Zone222"),
    ("Lithuania", "Euclid314"), ("Lithuania", "kepalaslt"), ("Lithuania", "kreukle"),
    ("Lithuania", "KurKestutis"), ("Lithuania", "Liudeselis"), ("Lithuania", "Mashinele"),
    ("Lithuania", "Melavau"), ("Lithuania", "Muzhachello"), ("Lithuania", "Xocas"),
    ("Malaysia", "Neutrals01"), ("Malaysia", "adrianalex86"), ("Malaysia", "James Boon"),
    ("Malaysia", "JXIV"), ("Malaysia", "kaika87"), ("Malaysia", "kimrhyme"),
    ("Malaysia", "Logan Ghanesh"), ("Malaysia", "RamboPang43200"),
    ("Mexico", "cben"), ("Mexico", "ComplixVandh"), ("Mexico", "danielayala94"),
    ("Mexico", "IMD5"), ("Mexico", "Lichidakiller"), ("Mexico", "manarori"),
    ("Mexico", "Palhinha1"), ("Mexico", "pchan19"), ("Mexico", "SamuelAroche"),
    ("Mexico", "vapula27"),
    ("Netherlands", "Bekse"), ("Netherlands", "de Slijp"),
    ("Netherlands", "G0dfriedvanBouillon"), ("Netherlands", "JohnNoordwijk"),
    ("Netherlands", "Loose-Goose"), ("Netherlands", "mart1nator"),
    ("Netherlands", "MeepleoverPeople"), ("Netherlands", "Ruadhh"),
    ("Netherlands", "Zwollywood"),
    ("Peru", "-Horse"), ("Peru", "-Nari-"), ("Peru", "AndreeMC"),
    ("Peru", "Eymicienta04"), ("Peru", "fli_pao"), ("Peru", "GonzaloMaizA"),
    ("Peru", "SANAQUI"), ("Peru", "spakune"), ("Peru", "XmikeZ"),
    ("Poland", "Gr3yDeath"), ("Poland", "Krzysztof285"), ("Poland", "kspttw"),
    ("Poland", "Lady Deer"), ("Poland", "Merlin_89"), ("Poland", "milo300IQ"),
    ("Poland", "poopzone"), ("Poland", "pricci"), ("Poland", "TerranPL"),
    ("Poland", "UY_Scuti"),
    ("Portugal", "andremiguelaa"), ("Portugal", "DiogoAntunes"), ("Portugal", "Glovir"),
    ("Portugal", "leodavinci"), ("Portugal", "Licinio Ferreira"), ("Portugal", "notnewatthis"),
    ("Portugal", "Paiva"), ("Portugal", "phoam"), ("Portugal", "Priskus"),
    ("Portugal", "Sportr"),
    ("RCP", "71st"), ("RCP", "AnnenMay"), ("RCP", "BBKosta"), ("RCP", "Bestwall"),
    ("RCP", "DexterLogan"), ("RCP", "Fox1347"), ("RCP", "isloun"),
    ("RCP", "Mikhail-rus"), ("RCP", "Someone_you_know"), ("RCP", "Tarakanov28"),
    ("Romania", "Ervin_G"), ("Romania", "g3rappa"), ("Romania", "game_over"),
    ("Romania", "Geo100"), ("Romania", "inicolae"), ("Romania", "IoanaFelicia"),
    ("Romania", "Mars15"), ("Romania", "mihnea27"), ("Romania", "tabado"),
    ("Spain", "FEIFER90"), ("Spain", "Kingkelodeon"), ("Spain", "Loku_elo"),
    ("Spain", "MadCan"), ("Spain", "migcrack"), ("Spain", "omendiez"),
    ("Spain", "oscaridis"), ("Spain", "Temrak"), ("Spain", "texe1"),
    ("Spain", "valle13"),
    ("Sweden", "Helge_H"), ("Sweden", "Jazzkatten"), ("Sweden", "jonben9603"),
    ("Sweden", "Modig fruktsamlare"), ("Sweden", "Moster84"), ("Sweden", "Quabatrarz"),
    ("Sweden", "skvallret"), ("Sweden", "theheras"),
    ("Taiwan", "0607"), ("Taiwan", "chengfengtsai"), ("Taiwan", "ezitakuto_TRAPS"),
    ("Taiwan", "HotlyHotly"), ("Taiwan", "hsieh david"), ("Taiwan", "K_Yuuki"),
    ("Taiwan", "Kin1218"), ("Taiwan", "Mrwan1"), ("Taiwan", "StevenChangTW"),
    ("Taiwan", "thunder_knight"),
    ("Thailand", "chawin053"), ("Thailand", "gorufu__"), ("Thailand", "GUN007"),
    ("Thailand", "idinidad"), ("Thailand", "Lztecih"), ("Thailand", "Natpeera-ch"),
    ("Thailand", "Pakshe"), ("Thailand", "pMika"), ("Thailand", "TAMATAMAz"),
    ("Thailand", "Tepphayak"),
    ("Ukraine", "A1egat0r"), ("Ukraine", "adrear"), ("Ukraine", "bazilyuk"),
    ("Ukraine", "Chemimaru"), ("Ukraine", "Lawyer"), ("Ukraine", "Random_23"),
    ("Ukraine", "Smile-"), ("Ukraine", "Tor_UA"), ("Ukraine", "trinidadec"),
    ("Ukraine", "zaharik"),
    ("United Kingdom", "corelli"), ("United Kingdom", "FanaticalLime93"),
    ("United Kingdom", "LazyBore"), ("United Kingdom", "Lord Terrycloth"),
    ("United Kingdom", "ohlookcake"), ("United Kingdom", "OJH1997"),
    ("United Kingdom", "small_red_van"), ("United Kingdom", "statmatt"),
    ("United Kingdom", "ted the notty bear"), ("United Kingdom", "wallaceprime"),
    ("United States", "Aliyoo"), ("United States", "BayAreaRube"),
    ("United States", "carky"), ("United States", "crocodilefundy"),
    ("United States", "dkl116"), ("United States", "GoldenBubbleface"),
    ("United States", "jpiekut"), ("United States", "justinethier"),
    ("United States", "Sheldahl"), ("United States", "zacbell"),
    ("Uruguay", "BDDL1"), ("Uruguay", "berna_uy"), ("Uruguay", "Federikus"),
    ("Uruguay", "fuchin86"), ("Uruguay", "Germancito"), ("Uruguay", "Mariaaf"),
    ("Uruguay", "Mendeleiev"), ("Uruguay", "Patablanca-uy"), ("Uruguay", "pikitina"),
    ("Uruguay", "porrasv"),
    ("Vietnam", "Bii1208"), ("Vietnam", "chicuongcoconut"),
    ("Vietnam", "dareyoutofightme"), ("Vietnam", "EriHerica1412"),
    ("Vietnam", "Meiasmay"), ("Vietnam", "portgard"), ("Vietnam", "stealcatfood"),
    ("Vietnam", "stealdogfood"), ("Vietnam", "VerKa148"), ("Vietnam", "Wolf Ren"),
]


def main():
    conn = duckdb.connect(str(DB_PATH))
    matched_ids = set()
    unmatched = []
    ambiguous = []

    for country_name, player_name in ENTRIES:
        expected_country = COUNTRY_MAP.get(country_name)
        rows = conn.execute(
            "SELECT id, name, country FROM players WHERE LOWER(name) = LOWER(?)",
            [player_name],
        ).fetchall()

        if not rows:
            unmatched.append((country_name, player_name))
            continue

        if len(rows) == 1:
            matched_ids.add(rows[0][0])
            continue

        # Multiple rows with same name — prefer country match
        country_matches = [r for r in rows if r[2] == expected_country]
        if len(country_matches) == 1:
            matched_ids.add(country_matches[0][0])
        else:
            ambiguous.append((country_name, player_name, rows))

    print(f"Matched: {len(matched_ids)} / {len(ENTRIES)}")
    print(f"Unmatched: {len(unmatched)}")
    print(f"Ambiguous: {len(ambiguous)}")

    if matched_ids:
        ids_list = list(matched_ids)
        placeholders = ",".join(["?"] * len(ids_list))
        conn.execute(
            f"UPDATE players SET national_team = TRUE WHERE id IN ({placeholders})",
            ids_list,
        )

    total_flagged = conn.execute(
        "SELECT COUNT(*) FROM players WHERE national_team = TRUE"
    ).fetchone()[0]
    print(f"Total national_team = TRUE now: {total_flagged}")

    if unmatched:
        print("\n--- Unmatched ---")
        for c, n in unmatched:
            print(f"  {c} | {n}")
    if ambiguous:
        print("\n--- Ambiguous (multiple DB rows, no country match) ---")
        for c, n, rows in ambiguous:
            print(f"  {c} | {n}: {rows}")

    conn.close()


if __name__ == "__main__":
    main()

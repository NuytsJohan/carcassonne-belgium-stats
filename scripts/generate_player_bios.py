"""Generate an English bio page (Markdown) for every Belgian national team player.

A player counts as national team player when they played at least one
nations_matches duel for Belgium (players.national_team is also set for
foreign WTCOC opponents, so it is not used here).

Writes one file per player to docs/bios/<slug>.md plus docs/bios/README.md.

Sections per page: Bio, Honours, Key figures, National team, Rivalries.

Hand-maintained pages: a file whose first line is `<!-- manual -->` is never
overwritten (e.g. obiwonder.md, which holds facts not yet in the database).

Facts not in the database can be added per player in docs/bios/overrides.json:
    {"<nickname>": {"bio": ["extra paragraph", ...],
                    "honours": [{"year": 2026, "tournament": "...", "result": "..."}]}}

Idempotent: rewrites every non-manual page on each run.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).parents[1]
DB_PATH = ROOT / "data" / "carcassonne.duckdb"
OUT_DIR = ROOT / "docs" / "bios"
OVERRIDES_PATH = OUT_DIR / "overrides.json"
MANUAL_MARKER = "<!-- manual -->"
UPDATED_RE = re.compile(r"\n\n\*Last updated: [^*\n]+\*(?=\n)")

MAX_RIVALS = 8
MIN_RIVAL_MATCHES = 5

# Dutch notes in tournament_participants → English honours text.
NOTE_EN = {
    "Kwartfinale bereikt": "quarter-final",
    "Eerste Belgische WK-deelname": "Belgium's first World Championship",
    "BRONS - Beste Belgische WK-resultaat": "bronze, best Belgian result at the time",
    "BRONS - Evenaart beste Belgische resultaat": "bronze, equalling the best Belgian result",
}

BCL_LEAGUES = {"ML": "Master League", "GL": "Gold League", "SL": "Silver League", "BL": "Bronze League"}


def ordinal(n: int) -> str:
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def long_date(d: date) -> str:
    return f"{d.day} {d:%B %Y}"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def join_words(items: list[str]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def thousands(n: int) -> str:
    return f"{n:,}"


def record(w: int, l: int, d: int | None = None) -> str:
    return f"{w}–{l}" if d is None else f"{w}–{l}–{d}"


def country(name: str) -> str:
    return name.replace("*", "").strip()


def load_overrides() -> dict:
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    return {}


def fetch_dicts(con, sql: str, params=()) -> list[dict]:
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Data ────────────────────────────────────────────────────────────────────

def load_team(con) -> list[dict]:
    return fetch_dicts(con, """
        SELECT p.id, p.name, p.name_nl,
               r.rank AS rank, r.total_score, r.ranking_elo,
               r.bga_peak_elo, r.bga_current_elo, r.bga_games, r.computed_at,
               h.rank AS h2h_rank, h.rating AS h2h_rating, h.peak_rating AS h2h_peak,
               h.peak_date AS h2h_peak_date, h.games AS h2h_games,
               h.wins AS h2h_wins, h.losses AS h2h_losses, h.draws AS h2h_draws
        FROM players p
        LEFT JOIN player_ranking r ON r.player_id = p.id
        LEFT JOIN player_head2head_elo h ON h.player_id = p.id
        WHERE EXISTS (SELECT 1 FROM nations_matches m WHERE m.player_id = p.id)
        ORDER BY p.id
    """)


def load_results(con, pid: int) -> list[dict]:
    return fetch_dicts(con, """
        SELECT t.name, t.type, t.year, t.participants_count AS field,
               tp.final_rank, tp.duels_played, tp.duels_won, tp.note
        FROM tournament_participants tp
        JOIN tournaments t ON t.id = tp.tournament_id
        WHERE tp.player_id = ? AND tp.final_rank IS NOT NULL
        ORDER BY t.year, t.name
    """, (pid,))


def load_nations(con, pid: int) -> list[dict]:
    return fetch_dicts(con, """
        SELECT t.name AS tournament, t.type, t.year, d.stage, d.opponent_country,
               d.date_played, m.score_belgium, m.score_opponent, m.result
        FROM nations_matches m
        JOIN nations_competition_duels d ON d.id = m.duel_id
        JOIN tournaments t ON t.id = d.tournament_id
        WHERE m.player_id = ?
        ORDER BY d.date_played, d.id
    """, (pid,))


def load_rivals(con, pid: int) -> list[dict]:
    return fetch_dicts(con, """
        SELECT p.name, p.name_nl, h.rank AS h2h_rank, count(*) AS n,
               sum((e.result = 1)::INT) AS w, sum((e.result = 0)::INT) AS l,
               sum((e.result NOT IN (0, 1))::INT) AS d
        FROM player_head2head_events e
        JOIN players p ON p.id = e.opponent_id
        LEFT JOIN player_head2head_elo h ON h.player_id = p.id
        WHERE e.player_id = ?
        GROUP BY 1, 2, 3
        HAVING count(*) >= ?
        ORDER BY n DESC, w DESC, lower(p.name)
    """, (pid, MIN_RIVAL_MATCHES))


def caps_ranking(con) -> dict[int, int]:
    rows = con.execute("""
        SELECT player_id, rank() OVER (ORDER BY count(*) DESC)
        FROM nations_matches GROUP BY player_id
    """).fetchall()
    return dict(rows)


# ── Honours ─────────────────────────────────────────────────────────────────

def bk_result(rank: int) -> str:
    return {1: "**Champion**", 2: "Runner-up"}.get(rank, ordinal(rank))


def honours_rows(results: list[dict]) -> list[dict]:
    """Notable results: BK top 3, every World Championship, BCOC top 4, BCL Master League
    finalists and other BCL league winners.

    The best BK and BCOC finish is always listed, even outside those cut-offs.
    """
    rows = []
    by_type = defaultdict(list)
    for r in results:
        by_type[r["type"]].append(r)

    def best(rs):
        return min(rs, key=lambda r: (r["final_rank"], -r["year"])) if rs else None

    best_bk, best_bcoc = best(by_type["BCLC"]), best(by_type["BCOC"])

    for r in by_type["BCLC"]:
        if r["final_rank"] <= 3 or r is best_bk:
            rows.append({"year": r["year"], "tournament": "Belgian Championship (live)",
                         "result": bk_result(r["final_rank"])})
    for r in by_type["WCC"]:
        res = f"{ordinal(r['final_rank'])} of {r['field']}" if r["field"] else ordinal(r["final_rank"])
        note = NOTE_EN.get(r["note"] or "")
        rows.append({"year": r["year"], "tournament": "World Championship",
                     "result": f"{res} ({note})" if note else res})
    for r in by_type["BCOC"]:
        if r["final_rank"] <= 4 or r is best_bcoc:
            rows.append({"year": r["year"], "tournament": "Belgian Carcassonne Online Championship",
                         "result": bk_result(r["final_rank"])})
    for r in by_type["BCL"]:
        m = re.match(r"BCL (\d{4}) (\w+) (ML|GL|SL|BL)", r["name"])
        if not m:
            continue
        season, tier = m.group(2), m.group(3)
        if tier == "ML":
            # Only the two group winners matter: they meet in the final for ranks 1-2.
            if r["final_rank"] > 2:
                continue
            res = ("**Winner** (group winner, won the final)" if r["final_rank"] == 1
                   else "Runner-up (group winner, lost the final)")
        elif r["final_rank"] == 1:
            res = "**Winner**"
            if r["duels_played"]:
                res += f" (won {r['duels_won']} of {r['duels_played']} matches)"
        else:
            continue
        rows.append({"year": r["year"], "tournament": f"BCL {season} ({BCL_LEAGUES[tier]})", "result": res})
    return rows


# ── Page ────────────────────────────────────────────────────────────────────

def render(p: dict, results: list[dict], nations: list[dict], rivals: list[dict],
           caps_rank: int, team_size: int, extra: dict) -> str:
    nick = p["name"]
    full = p["name_nl"]
    by_type = defaultdict(list)
    for r in results:
        by_type[r["type"]].append(r)
    bk, wcc, bcoc = by_type["BCLC"], by_type["WCC"], by_type["BCOC"]
    titles = [r["year"] for r in bk if r["final_rank"] == 1]
    online_titles = [r["year"] for r in bcoc if r["final_rank"] == 1]
    runner_ups = [r["year"] for r in bk if r["final_rank"] == 2]

    n_w = sum(m["result"] == "W" for m in nations)
    n_l = len(nations) - n_w
    official = [m for m in nations if m["type"] != "FRIENDLIES"]
    friendly = [m for m in nations if m["type"] == "FRIENDLIES"]
    debut = min(m["year"] for m in official) if official else min(
        m["date_played"].year for m in friendly if m["date_played"])

    lines = [f"# {full} ({nick}) — Carcassonne bio" if full else f"# {nick} — Carcassonne bio", ""]

    # Bio
    intro = f"{full}, known online as **{nick}**," if full else f"**{nick}**"
    def champion(kind: str, years: list[int]) -> str:
        n = f"{len(years)}-time " if len(years) > 1 else ""
        return f"**{n}{kind} champion** ({join_words([str(y) for y in years])})"

    rank_sep = " and"
    if titles or online_titles:
        won = []
        if titles:
            won.append(champion("Belgian", titles))
        if online_titles:
            won.append(champion("Belgian online", online_titles))
        head = "is a " + " and ".join(won)
        if len(won) > 1:
            rank_sep = ", and"
    elif runner_ups:
        head = f"was **runner-up at the Belgian Championship** in {join_words([str(y) for y in runner_ups])}"
    elif bk:
        b = min(bk, key=lambda r: (r["final_rank"], -r["year"]))
        head = f"finished {ordinal(b['final_rank'])} at the Belgian Championship in {b['year']} as best result"
    else:
        head = "plays for the Belgian national team"
    p1 = f"{intro} {head}"
    p1 += f"{rank_sep} ranks **{ordinal(p['rank'])} in the Belgian ranking**." if p["rank"] else "."
    if bk:
        p1 += (f" {nick} has played {len(bk)} editions of the live Belgian Championship"
               f" since {min(r['year'] for r in bk)}.")

    p2 = []
    if wcc:
        bw = min(wcc, key=lambda r: (r["final_rank"], r["year"]))
        times = "once" if len(wcc) == 1 else f"{len(wcc)} times"
        best_txt = f"**{ordinal(bw['final_rank'])} in {bw['year']}**"
        p2.append(f"{nick} has represented Belgium at the World Championship {times}"
                  + (f", with {best_txt} as best result." if len(wcc) > 1 else f", finishing {best_txt}."))
    pct = round(100 * n_w / len(nations)) if nations else 0
    p2.append(f"Since {debut}, {nick} has played {len(nations)} international "
              f"{'match' if len(nations) == 1 else 'matches'} for Belgium and won {n_w} ({pct}%).")
    if p["bga_games"]:
        p2.append(f"On Board Game Arena, {nick} has played {thousands(p['bga_games'])} rated games, "
                  f"with a peak rating of {int(p['bga_peak_elo'])}.")

    lines += ["## Bio", "", p1, "", " ".join(p2), ""]
    for para in extra.get("bio", []):
        lines += [para, ""]

    # Honours
    hon = honours_rows(results) + extra.get("honours", [])
    hon.sort(key=lambda r: -r["year"])
    if hon:
        bits = []
        if titles:
            bits.append(f"{len(titles)} Belgian title{'s' if len(titles) > 1 else ''}")
        if online_titles:
            bits.append(f"{len(online_titles)} online title{'s' if len(online_titles) > 1 else ''}")
        if runner_ups:
            bits.append(f"{len(runner_ups)} runner-up finish{'es' if len(runner_ups) > 1 else ''}")
        if wcc:
            bits.append(f"{len(wcc)} World Championship appearance{'s' if len(wcc) > 1 else ''}")
        verb = "stands" if len(bits) == 1 and bits[0].startswith("1 ") else "stand"
        lead = (f"{join_words(bits)} {verb} out in {nick}'s record." if bits
                else f"Best tournament results for {nick}.")
        lines += ["## Honours", "", lead, "", "| Year | Tournament | Result |", "| --- | --- | --- |"]
        lines += [f"| {r['year']} | {r['tournament']} | {r['result']} |" for r in hon]
        tail = []
        if bk:
            tail.append(f"{len(bk)} edition{'s' if len(bk) > 1 else ''} of the live Belgian Championship")
        if bcoc:
            b = min(bcoc, key=lambda r: (r["final_rank"], -r["year"]))
            best_txt = "champion" if b["final_rank"] == 1 else ordinal(b["final_rank"])
            tail.append(f"{len(bcoc)} edition{'s' if len(bcoc) > 1 else ''} of the online championship "
                        f"(best: {best_txt} in {b['year']})")
        if tail:
            lines += ["", f"Played: {join_words(tail)}."]
        lines.append("")

    # Key figures
    rows = []
    if p["rank"]:
        rows.append(("Belgian ranking", f"{ordinal(p['rank'])} ({round(p['total_score'])} points, "
                                        f"ranking rating {p['ranking_elo']})"))
    if p["h2h_rank"]:
        peak = f", peak {round(p['h2h_peak'])} on {p['h2h_peak_date'].day} {p['h2h_peak_date']:%b %Y}" if p["h2h_peak_date"] else ""
        rows.append(("Head-to-head rating", f"{ordinal(p['h2h_rank'])} ({round(p['h2h_rating'])}{peak})"))
        g = p["h2h_games"]
        rows.append(("Head-to-head matches", f"{g}: {p['h2h_wins']} won, {p['h2h_losses']} lost, "
                                             f"{p['h2h_draws']} drawn ({round(100 * p['h2h_wins'] / g)}% won)"))
    if p["bga_games"]:
        rows.append(("BGA rating", f"{int(p['bga_current_elo'])} (peak {int(p['bga_peak_elo'])})"))
        rows.append(("Rated BGA games", thousands(p["bga_games"])))
    rows.append(("International matches for Belgium", f"{len(nations)} ({n_w} won, {n_l} lost)"))
    lines += ["## Key figures", "", "| Measure | Value |", "| --- | --- |"]
    lines += [f"| {k} | {v} |" for k, v in rows]
    lines.append("")

    # National team
    lines += ["## National team", ""]
    o_w = sum(m["result"] == "W" for m in official)
    lead = (f"{nick} has won {o_w} of {len(official)} matches at official team championships"
            if official else f"{nick} has played only friendly internationals so far")
    lead += f" and is the {ordinal(caps_rank)} most-capped of {team_size} Belgian players."
    lines += [lead, ""]
    if official:
        lines += ["| Tournament | Won–lost | Highlights |", "| --- | --- | --- |"]
        per = defaultdict(list)
        for m in official:
            per[(m["year"], m["tournament"])].append(m)
        for (year, name), ms in sorted(per.items(), reverse=True):
            w = sum(m["result"] == "W" for m in ms)
            wins = [country(m["opponent_country"]) for m in ms if m["result"] == "W"]
            losses = [m for m in ms if m["result"] != "W" and (m["score_belgium"] or m["score_opponent"])]
            if wins:
                hl = f"Win{'s' if len(wins) > 1 else ''} against {join_words(wins[:3])}"
                if len(wins) > 3:
                    hl += f" and {len(wins) - 3} more"
            elif losses:
                c = min(losses, key=lambda m: m["score_opponent"] - m["score_belgium"])
                hl = (f"Narrowest loss: {country(c['opponent_country'])} "
                      f"({c['score_belgium']}–{c['score_opponent']})")
            else:
                hl = ""
            lines.append(f"| {name} | {record(w, len(ms) - w)} | {hl} |")
        lines.append("")
    tail = []
    if friendly:
        f_w = sum(m["result"] == "W" for m in friendly)
        tail.append(f"In friendly internationals the record is {record(f_w, len(friendly) - f_w)}.")
    top = max(nations, key=lambda m: m["score_belgium"] or 0)
    if top["score_belgium"]:
        where = top["tournament"] if top["type"] != "FRIENDLIES" else "a friendly"
        tail.append(f"Highest score for Belgium: {top['score_belgium']} points, against "
                    f"{country(top['opponent_country'])} ({where}).")
    if tail:
        lines += [" ".join(tail), ""]

    # Rivalries
    if rivals:
        lines += ["## Rivalries", ""]
        top10 = [r for r in rivals if r["h2h_rank"] and r["h2h_rank"] <= 10 and r["name"] != nick]
        best = max(top10, key=lambda r: ((r["w"] - r["l"]) / r["n"], r["n"]), default=None)
        most = rivals[0]
        lead = (f"{nick}'s most frequent opponent is {most['name']} "
                f"({most['n']} matches, {record(most['w'], most['l'], most['d'])}).")
        if best and best["w"] > best["l"] and best is not most:
            lead += (f" Against the top 10 the best record is versus {best['name']} "
                     f"(no. {best['h2h_rank']} head-to-head): {record(best['w'], best['l'], best['d'])}.")
        lines += [lead, "", "| Opponent | Matches | Won–lost–drawn |", "| --- | --- | --- |"]
        for r in rivals[:MAX_RIVALS]:
            label = f"{r['name']} ({r['name_nl']})" if r["name_nl"] and r["name_nl"] != r["name"] else r["name"]
            lines.append(f"| {label} | {r['n']} | {record(r['w'], r['l'], r['d'])} |")
        lines.append("")

    asof = f"{p['computed_at'].day} {p['computed_at']:%B %Y}" if p["computed_at"] else "the last ranking run"
    lines.append(f"*Source: Carcassonne Belgium statistics database (Belgian Championship, online championship, "
                 f"league, World Championship and international matches), rankings computed on {asof}. "
                 f"Generated by scripts/generate_player_bios.py.*")
    return "\n".join(lines) + "\n"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    overrides = load_overrides()
    team = load_team(con)
    caps = caps_ranking(con)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    index = []
    for p in team:
        path = OUT_DIR / f"{slugify(p['name'])}.md"
        nations = load_nations(con, p["id"])
        index.append((caps[p["id"]], p, path.name, len(nations)))
        if path.exists() and path.read_text(encoding="utf-8").startswith(MANUAL_MARKER):
            print(f"  skip (manual)  {path.name}")
            continue
        page = render(p, load_results(con, p["id"]), nations, load_rivals(con, p["id"]),
                      caps[p["id"]], len(team), overrides.get(p["name"], {}))
        old = path.read_text(encoding="utf-8") if path.exists() else ""
        if UPDATED_RE.search(old) and UPDATED_RE.sub("", old) == page:
            print(f"  unchanged      {path.name}")
            continue
        # The date goes right under the title; it only moves when the content changes.
        title, rest = page.split("\n", 1)
        path.write_text(f"{title}\n\n*Last updated: {long_date(date.today())}*\n{rest}", encoding="utf-8")
        print(f"  wrote          {path.name}")

    lines = ["# National team player bios", "",
             "One page per Belgian national team player, sorted by international matches played. "
             "Regenerate with `python scripts/generate_player_bios.py`; pages starting with "
             f"`{MANUAL_MARKER}` are maintained by hand.", "",
             "| Player | Name | Matches | Belgian ranking |", "| --- | --- | --- | --- |"]
    for _, p, fname, n in sorted(index, key=lambda x: (x[0], x[1]["name"].lower())):
        rank = ordinal(p["rank"]) if p["rank"] else ""
        lines.append(f"| [{p['name']}]({fname}) | {p['name_nl'] or ''} | {n} | {rank} |")
    (OUT_DIR / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nDone. {len(team)} players, index at {OUT_DIR / 'README.md'}.")


if __name__ == "__main__":
    main()

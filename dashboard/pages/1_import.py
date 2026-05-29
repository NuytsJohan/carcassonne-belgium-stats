"""
BGA Data Import pagina.
Haalt Carcassonne speldata op van BoardGameArena en importeert in DuckDB.
De volledige import draait in een achtergrondthread zodat Streamlit reruns
de import niet onderbreken.

Threading-aanpak: st.session_state is niet thread-safe in nieuwere Streamlit.
We slaan een gewone dict op in session_state en geven die mee als referentie
aan de thread. De thread schrijft naar die dict, de UI leest hem bij elke rerun.
"""

import asyncio
import os
import sys
import threading
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

if not os.environ.get("CARCASSONNE_ADMIN"):
    st.error("This page is not available.")
    st.stop()

import duckdb

sys.path.insert(0, str(Path(__file__).parents[2]))

from src.importers.bga_fetcher import get_token_and_cookies, fetch_player_games, fetch_player_country
from src.importers.bga_importer import (
    import_game,
    _backfill_importing_player_elo,
    _parse_importing_player_elo,
    DB_PATH,
)

ROOT    = Path(__file__).parents[2]
DB_FILE = str(ROOT / DB_PATH)

st.title("📥 Import BGA Data")
st.caption("Fetch game data from BoardGameArena and import into the database")


# ── Thread-safe shared state ─────────────────────────────────────────────────
# Gewone Python dicts/lists, opgeslagen in session_state maar alleen via
# referentie benaderd door de thread (geen st.session_state[] in de thread).

if "import_state" not in st.session_state:
    st.session_state["import_state"] = {
        "running": False, "done": False, "total": 0, "log": [],
    }

if "fix_arena_state" not in st.session_state:
    st.session_state["fix_arena_state"] = {
        "running": False, "done": False, "log": [],
    }

if "country_state" not in st.session_state:
    st.session_state["country_state"] = {
        "running": False, "done": False, "updated": 0, "log": [],
    }

if "elo_backfill_state" not in st.session_state:
    st.session_state["elo_backfill_state"] = {
        "running": False, "done": False, "updated": 0, "log": [],
    }

imp = st.session_state["import_state"]
arena = st.session_state["fix_arena_state"]
cstate = st.session_state["country_state"]
elostate = st.session_state["elo_backfill_state"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run_async(coro):
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── Achtergrond import functie ────────────────────────────────────────────────

def run_import(email, password, player_ids, s):
    """s = shared state dict (niet st.session_state)."""

    def log(msg):
        s["log"].append(msg)

    try:
        log("🔑 Logging into BGA ...")
        token, cookies = _run_async(
            get_token_and_cookies(email, password, player_ids[0], headless=True)
        )
        log(f"✅ Token: {token[:8]}... obtained")

        conn = duckdb.connect(DB_FILE)
        for mig in ["migrations/001_initial_schema.sql", "migrations/002_bga_fields.sql", "migrations/003_game_extra_fields.sql", "migrations/005_boardgames.sql"]:
            sql = (ROOT / mig).read_text(encoding="utf-8")
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(stmt)
                    except Exception:
                        pass

        total_new = 0
        for pid in player_ids:
            bga_pid_str = str(pid)
            # Normale cutoff = laatste game waarin de speler voorkomt (incrementeel).
            track_row = conn.execute("""
                SELECT last_ended_at FROM import_tracking
                WHERE bga_player_id = ? AND boardgame_id = 1
            """, [bga_pid_str]).fetchone()
            tracked_since = track_row[0] if track_row and track_row[0] else None

            # Maar als er games zijn met ontbrekende elo voor deze speler (binnengekomen
            # via een tegenstander, daarom NULL), moeten we ver genoeg terug fetchen om
            # ze opnieuw te bezoeken zodat import_game de missende velden backfilt.
            # Daarom: ga terug tot de OUDSTE game zonder elo (met echte table_id) van
            # het laatste jaar. Oude residu-records (placeholders zonder table_id)
            # negeren we.
            gap_row = conn.execute("""
                SELECT MIN(COALESCE(g.ended_at, g.played_at))
                FROM game_players gp
                JOIN games g ON g.id = gp.game_id
                JOIN players p ON p.id = gp.player_id
                WHERE p.bga_player_id = ?
                  AND gp.elo_after IS NULL
                  AND g.bga_table_id IS NOT NULL
                  AND g.unranked = false
                  AND COALESCE(g.ended_at, g.played_at) >= ?
            """, [bga_pid_str, datetime.now() - timedelta(days=365)]).fetchone()
            gap_since = gap_row[0] if gap_row and gap_row[0] else None

            if tracked_since and gap_since:
                since = min(tracked_since, gap_since)
            else:
                since = gap_since or tracked_since

            if since:
                log(f"👤 Fetching player {pid} (since {since.date()}) ...")
            else:
                log(f"👤 Fetching player {pid} (full history) ...")
            try:
                games = fetch_player_games(pid, token, cookies, since=since)
                log(f"   📊 {len(games)} games found")
                new = sum(1 for g in games if import_game(conn, g, importing_bga_pid=pid))
                total_new += new
                log(f"   ✅ {new} newly imported")

                max_ended = conn.execute("""
                    SELECT MAX(COALESCE(g.ended_at, g.played_at))
                    FROM games g
                    JOIN game_players gp ON gp.game_id = g.id
                    JOIN players p ON p.id = gp.player_id
                    WHERE p.bga_player_id = ?
                """, [bga_pid_str]).fetchone()
                new_last = max_ended[0] if max_ended and max_ended[0] else None
                if new_last:
                    conn.execute("""
                        INSERT INTO import_tracking (bga_player_id, boardgame_id, last_ended_at, imported_at)
                        VALUES (?, 1, ?, now())
                        ON CONFLICT (bga_player_id, boardgame_id)
                        DO UPDATE SET last_ended_at = EXCLUDED.last_ended_at,
                                      imported_at = now()
                    """, [bga_pid_str, new_last])
            except Exception as e:
                log(f"   ❌ Error: {e}\n{traceback.format_exc()}")

        conn.close()
        s["total"] = total_new
        log(f"🏁 Done — {total_new} new games added")

    except Exception as e:
        log(f"❌ Error: {e}\n{traceback.format_exc()}")
    finally:
        s["running"] = False
        s["done"] = True


# ── Arena correctie per speler ────────────────────────────────────────────────

def run_fix_arena(email, password, bga_player_id, player_name, s):
    """s = shared state dict (niet st.session_state)."""

    def log(msg):
        s["log"].append(msg)

    log(f"🚀 Arena correction started for {player_name} (BGA {bga_player_id})")

    try:
        log("🔑 Logging into BGA ...")
        token, cookies = _run_async(
            get_token_and_cookies(email, password, bga_player_id, headless=True)
        )
        log(f"✅ Token: {token[:8]}... obtained")

        log(f"👤 Fetching games for {player_name} ({bga_player_id}) ...")
        games = fetch_player_games(bga_player_id, token, cookies)
        log(f"   📊 {len(games)} games fetched")

        conn = duckdb.connect(DB_FILE)
        updated = 0
        for game in games:
            table_id = str(game.get("table_id", ""))
            arena_after_raw = game.get("arena_after")
            arena_win_raw = game.get("arena_win")

            if not table_id or arena_after_raw is None:
                continue

            raw_str = str(arena_after_raw)
            arena_elo = None
            if "." in raw_str:
                try:
                    arena_elo = int(raw_str.split(".")[1])
                except (ValueError, IndexError):
                    pass
            else:
                try:
                    arena_elo = int(raw_str)
                except (ValueError, TypeError):
                    pass

            arena_win = None
            if arena_win_raw is not None:
                win_str = str(arena_win_raw)
                if "." in win_str:
                    try:
                        arena_win = int(win_str.split(".")[0]) > 0
                    except (ValueError, IndexError):
                        pass
                elif win_str.isdigit():
                    arena_win = bool(int(win_str))

            if arena_elo is not None:
                result = conn.execute("""
                    UPDATE game_players
                    SET arena_after = ?, arena_win = ?
                    WHERE game_id = (SELECT id FROM games WHERE bga_table_id = ?)
                      AND player_id = (SELECT id FROM players WHERE bga_player_id = ?)
                """, [arena_elo, arena_win, table_id, str(bga_player_id)])
                if result.fetchone()[0] > 0:
                    updated += 1

        conn.close()
        log(f"🏁 Done — {updated} arena scores updated for {player_name}")

    except Exception as e:
        log(f"❌ Error: {e}\n{traceback.format_exc()}")
    finally:
        s["running"] = False
        s["done"] = True


# ── ELO backfill per speler ───────────────────────────────────────────────────

def run_elo_backfill(email, password, players, s):
    """Backfill ontbrekende elo_after/elo_win/elo_penalty per speler.

    players = lijst van (bga_player_id, name). Per speler fetchen we enkel het
    laatste jaar aan games — daar zitten de gaten die door het import-probleem
    ontstonden. Oude residu-gaten (placeholder records uit 2022-2024 zonder echte
    table_id) worden zo genegeerd. De ontbrekende velden worden ingevuld op die
    spelers eigen game_players rij.
    s = shared state dict (niet st.session_state).
    """

    def log(msg):
        s["log"].append(msg)

    since = datetime.now() - timedelta(days=365)

    try:
        log("🔑 Logging into BGA ...")
        token, cookies = _run_async(
            get_token_and_cookies(email, password, int(players[0][0]), headless=True)
        )
        log(f"✅ Token: {token[:8]}... obtained")

        conn = duckdb.connect(DB_FILE)
        total_updated = 0
        for bga_pid, name in players:
            bga_pid_str = str(bga_pid)
            log(f"👤 {name} ({bga_pid}) — fetching since {since.date()} ...")
            try:
                games = fetch_player_games(int(bga_pid), token, cookies, since=since)
                log(f"   📊 {len(games)} games fetched")
                updated = 0
                for game in games:
                    table_id = str(game.get("table_id", ""))
                    if not table_id:
                        continue
                    existing = conn.execute(
                        "SELECT id FROM games WHERE bga_table_id = ?", [table_id]
                    ).fetchone()
                    if not existing:
                        continue
                    if _backfill_importing_player_elo(
                        conn, existing[0], bga_pid_str,
                        _parse_importing_player_elo(game),
                    ):
                        updated += 1
                total_updated += updated
                log(f"   ✅ {updated} games backfilled")
            except Exception as e:
                log(f"   ❌ Error for {name}: {e}")

        conn.close()
        s["updated"] = total_updated
        log(f"🏁 Done — {total_updated} games backfilled across {len(players)} player(s)")

    except Exception as e:
        log(f"❌ Error: {e}\n{traceback.format_exc()}")
    finally:
        s["running"] = False
        s["done"] = True


# ── Country check ────────────────────────────────────────────────────────────

def run_country_check(email, password, s):
    """s = shared state dict."""

    def log(msg):
        s["log"].append(msg)

    try:
        conn = duckdb.connect(DB_FILE)
        players = conn.execute(
            "SELECT id, name, bga_player_id FROM players WHERE bga_player_id IS NOT NULL AND country IS NULL ORDER BY name"
        ).fetchall()
        conn.close()

        if not players:
            log("No players without country found.")
            return

        log(f"🔑 Logging into BGA ...")
        first_pid = int(players[0][2])
        token, cookies = _run_async(
            get_token_and_cookies(email, password, first_pid, headless=True)
        )
        log(f"✅ Token obtained — {len(players)} players to check\n")

        updated = 0
        for internal_id, name, bga_pid in players:
            fetched = fetch_player_country(int(bga_pid), token, cookies)
            country_to_set = fetched if fetched else "UNKNOWN"
            conn2 = duckdb.connect(DB_FILE)
            conn2.execute("UPDATE players SET country = ? WHERE id = ?", [country_to_set, internal_id])
            conn2.close()
            if fetched:
                log(f"  ✅ {name}: {fetched}")
                updated += 1
            else:
                log(f"  ⚠️  {name}: not found → UNKNOWN")

        s["updated"] = updated
        log(f"\n🏁 Done — {updated} countries updated")

    except Exception as e:
        log(f"❌ Error: {e}\n{traceback.format_exc()}")
    finally:
        s["running"] = False
        s["done"] = True


# ── UI ────────────────────────────────────────────────────────────────────────

with st.expander("⚙️ BGA Credentials", expanded=not imp["running"]):
    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("BGA Email", value=st.session_state.get("bga_email", ""), key="input_email")
    with col2:
        password = st.text_input("BGA Password", type="password", value=st.session_state.get("bga_password", ""), key="input_password")

# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_import, tab_fix_arena, tab_elo, tab_countries = st.tabs(
    ["📥 Import", "🎯 Arena correction", "📈 ELO backfill", "🌍 Countries"]
)

with tab_import:
    st.subheader("Import players")

    only_imported = st.checkbox("Only previously imported players", value=True, key="only_imported")
    only_nt_import = st.checkbox("National team", value=False, key="only_nt_import")

    try:
        conn_imp = duckdb.connect(DB_FILE, read_only=True)
        if only_imported:
            imported_players = conn_imp.execute("""
                SELECT p.bga_player_id, p.name,
                       COUNT(gp.id) AS games,
                       it.last_ended_at,
                       it.imported_at
                FROM players p
                JOIN game_players gp ON gp.player_id = p.id
                JOIN import_tracking it
                    ON it.bga_player_id = p.bga_player_id AND it.boardgame_id = 1
                WHERE p.bga_player_id IS NOT NULL
                  AND it.imported_at IS NOT NULL
                  AND (? = false OR p.national_team = true)
                GROUP BY p.bga_player_id, p.name, it.last_ended_at, it.imported_at
                ORDER BY p.name
            """, [only_nt_import]).df()
        else:
            imported_players = conn_imp.execute("""
                SELECT p.bga_player_id, p.name,
                       COUNT(gp.id) AS games,
                       it.last_ended_at,
                       it.imported_at
                FROM players p
                JOIN game_players gp ON gp.player_id = p.id
                LEFT JOIN import_tracking it
                    ON it.bga_player_id = p.bga_player_id AND it.boardgame_id = 1
                WHERE p.bga_player_id IS NOT NULL
                  AND (? = false OR p.national_team = true)
                GROUP BY p.bga_player_id, p.name, it.last_ended_at, it.imported_at
                ORDER BY p.name
            """, [only_nt_import]).df()
        conn_imp.close()
    except Exception:
        imported_players = None

    if imported_players is not None and not imported_players.empty:
        select_all = st.checkbox("Select all", value=False, key="import_select_all")

        edit_df = imported_players[["name", "bga_player_id", "games", "last_ended_at", "imported_at"]].copy()
        edit_df.insert(0, "✔", select_all)
        edit_df = edit_df.rename(columns={
            "name":           "Name",
            "bga_player_id":  "BGA ID",
            "games":          "Games",
            "last_ended_at":  "Last game",
            "imported_at":    "Last import",
        })

        result = st.data_editor(
            edit_df,
            width="stretch",
            hide_index=True,
            disabled=["Name", "BGA ID", "Games", "Last game", "Last import"],
            key="import_players_editor",
        )

        selected_pids = [
            int(imported_players.iloc[i]["bga_player_id"])
            for i, checked in enumerate(result["✔"])
            if checked
        ]

        st.caption(f"{len(selected_pids)} player(s) selected")

        # Add new player ID
        new_pid = st.text_input("Add new BGA Player ID", placeholder="e.g. 93464744")

        if not imp["running"]:
            if st.button("▶ Start import", type="primary"):
                player_ids = list(selected_pids)
                if new_pid and new_pid.strip().isdigit():
                    player_ids.append(int(new_pid.strip()))
                if not player_ids:
                    st.error("Select at least one player or add a new ID.")
                elif not email or not password:
                    st.error("Fill in email and password.")
                else:
                    st.session_state["bga_email"] = email
                    st.session_state["bga_password"] = password
                    imp["running"] = True
                    imp["done"] = False
                    imp["total"] = 0
                    imp["log"] = []

                    t = threading.Thread(
                        target=run_import,
                        args=(email, password, player_ids, imp),
                        daemon=True,
                    )
                    t.start()
                    st.rerun()
        else:
            st.info("⏳ Import running — page refreshes automatically ...")
            st.button("↻ Refresh status", on_click=lambda: None)
    else:
        # No existing players — show input field
        new_pid = st.text_input("BGA Player ID", placeholder="e.g. 93464744")

        if not imp["running"]:
            if st.button("▶ Start import", type="primary"):
                if not new_pid or not new_pid.strip().isdigit():
                    st.error("Enter a valid BGA Player ID.")
                elif not email or not password:
                    st.error("Fill in email and password.")
                else:
                    st.session_state["bga_email"] = email
                    st.session_state["bga_password"] = password
                    imp["running"] = True
                    imp["done"] = False
                    imp["total"] = 0
                    imp["log"] = []

                    t = threading.Thread(
                        target=run_import,
                        args=(email, password, [int(new_pid.strip())], imp),
                        daemon=True,
                    )
                    t.start()
                    st.rerun()
        else:
            st.info("⏳ Import running — page refreshes automatically ...")
            st.button("↻ Refresh status", on_click=lambda: None)

    if imp["log"]:
        st.code("\n".join(imp["log"][-40:]), language=None)

    if imp["done"] and not imp["running"]:
        st.success(f"✅ Import complete — {imp['total']} new games added")

# ── Tab 2: Arena correctie per speler ────────────────────────────────────────

with tab_fix_arena:
    st.subheader("Fix arena scores per player")
    st.caption("Select a player to re-fetch arena scores from BGA.")

    try:
        conn_players = duckdb.connect(DB_FILE, read_only=True)
        arena_players = conn_players.execute("""
            SELECT p.id, p.name, p.bga_player_id,
                   COUNT(gp.id) AS games,
                   SUM(CASE WHEN gp.arena_after IS NOT NULL THEN 1 ELSE 0 END) AS with_arena,
                   SUM(CASE WHEN gp.arena_after IS NULL THEN 1 ELSE 0 END) AS without_arena
            FROM players p
            JOIN game_players gp ON gp.player_id = p.id
            WHERE p.bga_player_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM game_players gp2
                  WHERE gp2.player_id = p.id AND gp2.elo_after IS NOT NULL
              )
            GROUP BY p.id, p.name, p.bga_player_id
            ORDER BY p.name
        """).fetchall()
        conn_players.close()
    except duckdb.ConnectionException:
        arena_players = []

    if arena_players:
        player_options = {
            f"{name} ({games} games, {with_arena} arena, {without_arena} without)": (bga_pid, name)
            for _id, name, bga_pid, games, with_arena, without_arena in arena_players
        }
        selected_player_label = st.selectbox("Player", list(player_options.keys()))
        selected_bga_pid, selected_name = player_options[selected_player_label]

        if not arena["running"]:
            if st.button("▶ Fix arena", type="primary"):
                if not email or not password:
                    st.error("Fill in email and password first (top of page).")
                else:
                    st.session_state["bga_email"] = email
                    st.session_state["bga_password"] = password
                    arena["running"] = True
                    arena["done"] = False
                    arena["log"] = []

                    t = threading.Thread(
                        target=run_fix_arena,
                        args=(email, password, int(selected_bga_pid), selected_name, arena),
                        daemon=True,
                    )
                    t.start()
                    st.rerun()
        else:
            st.info("⏳ Arena correction running ...")
            st.button("↻ Refresh status", on_click=lambda: None, key="fix_arena_refresh")

        if arena["log"]:
            st.code("\n".join(arena["log"][-40:]), language=None)

        if arena["done"] and not arena["running"]:
            st.success("✅ Arena correction complete")
    else:
        st.info("No players found in the database.")

# ── Tab 3: ELO backfill ──────────────────────────────────────────────────────

with tab_elo:
    st.subheader("Backfill missing ELO per player")
    st.caption(
        "Re-fetches each Belgian player from the most recent game that still has ELO "
        "(where the gap starts) and fills in missing elo_after/elo_win for games that "
        "were imported via an opponent (so the player's own ELO was NULL)."
    )

    try:
        conn_elo = duckdb.connect(DB_FILE, read_only=True)
        elo_players = conn_elo.execute("""
            SELECT p.bga_player_id, p.name,
                   COUNT(gp.id) AS games,
                   SUM(CASE WHEN gp.elo_after IS NULL THEN 1 ELSE 0 END) AS missing_elo
            FROM players p
            JOIN game_players gp ON gp.player_id = p.id
            JOIN games g ON g.id = gp.game_id AND g.unranked = false
            WHERE p.bga_player_id IS NOT NULL
              AND p.country = 'BE'
              AND EXISTS (
                  SELECT 1 FROM import_tracking it
                  WHERE it.bga_player_id = p.bga_player_id AND it.boardgame_id = 1
                    AND it.imported_at IS NOT NULL
              )
            GROUP BY p.bga_player_id, p.name
            HAVING SUM(CASE WHEN gp.elo_after IS NULL THEN 1 ELSE 0 END) > 0
            ORDER BY missing_elo DESC
        """).df()
        conn_elo.close()
    except Exception:
        elo_players = None

    if elo_players is not None and not elo_players.empty:
        elo_select_all = st.checkbox("Select all", value=False, key="elo_select_all")

        elo_edit_df = elo_players[["name", "bga_player_id", "games", "missing_elo"]].copy()
        elo_edit_df.insert(0, "✔", elo_select_all)
        elo_edit_df = elo_edit_df.rename(columns={
            "name":          "Name",
            "bga_player_id": "BGA ID",
            "games":         "Games",
            "missing_elo":   "Missing ELO",
        })

        elo_result = st.data_editor(
            elo_edit_df,
            width="stretch",
            hide_index=True,
            disabled=["Name", "BGA ID", "Games", "Missing ELO"],
            key="elo_players_editor",
        )

        elo_selected = [
            (int(elo_players.iloc[i]["bga_player_id"]), str(elo_players.iloc[i]["name"]))
            for i, checked in enumerate(elo_result["✔"])
            if checked
        ]
        st.caption(f"{len(elo_selected)} player(s) selected")

        if not elostate["running"]:
            if st.button("▶ Backfill ELO", type="primary", key="elo_backfill_btn"):
                if not elo_selected:
                    st.error("Select at least one player.")
                elif not email or not password:
                    st.error("Fill in email and password first (top of page).")
                else:
                    st.session_state["bga_email"] = email
                    st.session_state["bga_password"] = password
                    elostate["running"] = True
                    elostate["done"] = False
                    elostate["updated"] = 0
                    elostate["log"] = []
                    threading.Thread(
                        target=run_elo_backfill,
                        args=(email, password, elo_selected, elostate),
                        daemon=True,
                    ).start()
                    st.rerun()
        else:
            st.info("⏳ ELO backfill running — page refreshes automatically ...")
            st.button("↻ Refresh status", on_click=lambda: None, key="elo_backfill_refresh")

        if elostate["log"]:
            st.code("\n".join(elostate["log"][-40:]), language=None)

        if elostate["done"] and not elostate["running"]:
            st.success(f"✅ ELO backfill complete — {elostate['updated']} games backfilled")
    else:
        st.info("No players with missing ELO found.")


# ── Tab 4: Country check ─────────────────────────────────────────────────────

with tab_countries:
    st.subheader("Check countries")
    st.caption("Fetches the country from BGA for players without one (country IS NULL). Not found → UNKNOWN.")

    try:
        conn_co = duckdb.connect(DB_FILE, read_only=True)
        df_countries = conn_co.execute(
            "SELECT name AS Name, bga_player_id AS \"BGA ID\", country AS Country FROM players WHERE bga_player_id IS NOT NULL ORDER BY name"
        ).df()
        null_count = int(conn_co.execute(
            "SELECT COUNT(*) FROM players WHERE bga_player_id IS NOT NULL AND country IS NULL"
        ).fetchone()[0])
        conn_co.close()
        if not df_countries.empty:
            st.dataframe(df_countries, hide_index=True, use_container_width=True)
        st.caption(f"{null_count} player(s) without country")
    except Exception:
        null_count = 0

    if not cstate["running"]:
        if st.button("🔍 Fetch & fix countries", disabled=imp["running"] or arena["running"] or null_count == 0):
            if not email or not password:
                st.error("Fill in email and password first (top of page).")
            else:
                st.session_state["bga_email"] = email
                st.session_state["bga_password"] = password
                cstate["running"] = True
                cstate["done"] = False
                cstate["updated"] = 0
                cstate["log"] = []
                threading.Thread(
                    target=run_country_check,
                    args=(email, password, cstate),
                    daemon=True,
                ).start()
                st.rerun()
    else:
        st.info("⏳ Country check running ...")
        st.button("↻ Refresh", key="refresh_countries", on_click=lambda: None)

    if cstate["log"]:
        st.code("\n".join(cstate["log"][-60:]), language=None)

    if cstate["done"] and not cstate["running"]:
        st.success(f"✅ Country check complete — {cstate['updated']} countries updated")

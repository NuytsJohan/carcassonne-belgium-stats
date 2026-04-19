"""WCC (World Carcassonne Championship) tournament overview."""
from pathlib import Path

import duckdb
import streamlit as st

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"

st.title("🌍 WCC Tournaments")

if not DB_PATH.exists():
    st.warning("No database found.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# Display name: if we know both a real name (name_nl) and a distinct BGA
# handle (in `name`), show "Real Name (bga_handle)". If the only extra info
# is a row in player_aliases, fall back to appending that. Works for any
# country, not just Belgium.
DISPLAY_NAME = (
    "CASE "
    "  WHEN p.name_nl IS NOT NULL AND p.name_nl <> p.name "
    "    THEN p.name_nl || ' (' || p.name || ')' "
    "  WHEN pa.alias IS NOT NULL "
    "       AND pa.alias <> COALESCE(p.name_nl, p.name) "
    "    THEN COALESCE(p.name_nl, p.name) || ' (' || pa.alias || ')' "
    "  ELSE COALESCE(p.name_nl, p.name) "
    "END"
)

ALIAS_JOIN = (
    "LEFT JOIN (SELECT player_id, MIN(alias) AS alias "
    "           FROM player_aliases GROUP BY player_id) pa "
    "  ON pa.player_id = p.id"
)

editions = conn.execute("""
    SELECT id, year, location, participants_count, notes
    FROM tournaments
    WHERE type = 'WCC'
    ORDER BY year DESC
""").fetchall()

if not editions:
    st.info("No WCC tournaments found.")
    conn.close()
    st.stop()

year_options = ["All years"] + [r[1] for r in editions]
selected_year = st.selectbox("Edition", year_options)

# ── Summary metrics across ALL editions ─────────────────────────────────────
total_editions = len(editions)
total_participants = conn.execute("""
    SELECT COUNT(*) FROM tournament_participants tp
    JOIN tournaments t ON tp.tournament_id = t.id
    WHERE t.type = 'WCC'
""").fetchone()[0]
be_best = conn.execute("""
    SELECT MIN(tp.final_rank) FROM tournament_participants tp
    JOIN tournaments t ON tp.tournament_id = t.id
    JOIN players p ON p.id = tp.player_id
    WHERE t.type = 'WCC' AND p.country = 'BE' AND tp.final_rank IS NOT NULL
""").fetchone()[0]
be_participations = conn.execute("""
    SELECT COUNT(*) FROM tournament_participants tp
    JOIN tournaments t ON tp.tournament_id = t.id
    JOIN players p ON p.id = tp.player_id
    WHERE t.type = 'WCC' AND p.country = 'BE'
""").fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Editions", total_editions)
c2.metric("Recorded players", total_participants)
c3.metric("BE participations", be_participations)
c4.metric("Best BE rank", be_best if be_best else "—")

if selected_year == "All years":
    # ── Belgian participations ──────────────────────────────────────────────
    st.subheader("🇧🇪 Belgian participations")
    df_be = conn.execute(f"""
        SELECT
            t.year AS "Year",
            tp.final_rank AS "Rank",
            {DISPLAY_NAME} AS "Player",
            t.participants_count AS "Field",
            tp.note AS "Note"
        FROM tournament_participants tp
        JOIN tournaments t ON tp.tournament_id = t.id
        JOIN players p ON p.id = tp.player_id
        {ALIAS_JOIN}
        WHERE t.type = 'WCC' AND p.country = 'BE'
        ORDER BY t.year DESC, tp.final_rank
    """).df()
    st.dataframe(df_be, use_container_width=True, hide_index=True)

    # ── Hall of Champions ───────────────────────────────────────────────────
    st.subheader("Hall of Champions")
    df_champs = conn.execute(f"""
        SELECT
            t.year AS "Year",
            t.location AS "Location",
            t.participants_count AS "Players",
            MAX(CASE WHEN tp.final_rank = 1 THEN {DISPLAY_NAME} || ' (' || p.country || ')' END) AS "🥇 Champion",
            MAX(CASE WHEN tp.final_rank = 2 THEN {DISPLAY_NAME} || ' (' || p.country || ')' END) AS "🥈 Runner-up",
            MAX(CASE WHEN tp.final_rank = 3 THEN {DISPLAY_NAME} || ' (' || p.country || ')' END) AS "🥉 3rd",
            MAX(CASE WHEN tp.final_rank = 4 THEN {DISPLAY_NAME} || ' (' || p.country || ')' END) AS "4th",
            t.notes AS "Notes"
        FROM tournaments t
        LEFT JOIN tournament_participants tp ON tp.tournament_id = t.id
        LEFT JOIN players p ON p.id = tp.player_id
        {ALIAS_JOIN}
        WHERE t.type = 'WCC'
        GROUP BY t.id, t.year, t.location, t.participants_count, t.notes
        ORDER BY t.year DESC
    """).df()
    st.dataframe(df_champs, use_container_width=True, hide_index=True)

    # ── Country medal table ─────────────────────────────────────────────────
    st.subheader("Country medal table")
    df_countries = conn.execute("""
        SELECT
            p.country AS "Country",
            SUM(CASE WHEN tp.final_rank = 1 THEN 1 ELSE 0 END) AS "🥇",
            SUM(CASE WHEN tp.final_rank = 2 THEN 1 ELSE 0 END) AS "🥈",
            SUM(CASE WHEN tp.final_rank = 3 THEN 1 ELSE 0 END) AS "🥉",
            SUM(CASE WHEN tp.final_rank = 4 THEN 1 ELSE 0 END) AS "4th",
            SUM(CASE WHEN tp.final_rank <= 3 THEN 1 ELSE 0 END) AS "Medals"
        FROM tournament_participants tp
        JOIN tournaments t ON tp.tournament_id = t.id
        JOIN players p ON p.id = tp.player_id
        WHERE t.type = 'WCC' AND tp.final_rank <= 4
        GROUP BY p.country
        ORDER BY "🥇" DESC, "🥈" DESC, "🥉" DESC, "4th" DESC
    """).df()
    st.dataframe(df_countries, use_container_width=True, hide_index=True)

    # ── Top performers ──────────────────────────────────────────────────────
    st.subheader("Top performers")
    df_players = conn.execute(f"""
        SELECT
            {DISPLAY_NAME} AS "Player",
            p.country AS "Country",
            COUNT(*) AS "Editions",
            SUM(CASE WHEN tp.final_rank = 1 THEN 1 ELSE 0 END) AS "🥇",
            SUM(CASE WHEN tp.final_rank = 2 THEN 1 ELSE 0 END) AS "🥈",
            SUM(CASE WHEN tp.final_rank = 3 THEN 1 ELSE 0 END) AS "🥉",
            SUM(CASE WHEN tp.final_rank = 4 THEN 1 ELSE 0 END) AS "4th",
            MIN(tp.final_rank) AS "Best",
            STRING_AGG(CAST(t.year AS VARCHAR), ', ' ORDER BY t.year) AS "Years"
        FROM tournament_participants tp
        JOIN tournaments t ON tp.tournament_id = t.id
        JOIN players p ON p.id = tp.player_id
        {ALIAS_JOIN}
        WHERE t.type = 'WCC'
        GROUP BY p.id, p.name, p.name_nl, p.country, pa.alias
        HAVING COUNT(*) > 1 OR SUM(CASE WHEN tp.final_rank <= 4 THEN 1 ELSE 0 END) > 0
        ORDER BY "🥇" DESC, "🥈" DESC, "🥉" DESC, "4th" DESC, "Editions" DESC
    """).df()
    st.dataframe(df_players, use_container_width=True, hide_index=True)

    # ── Participation growth ────────────────────────────────────────────────
    st.subheader("Field size over time")
    df_growth = conn.execute("""
        SELECT year, participants_count
        FROM tournaments
        WHERE type = 'WCC' AND participants_count IS NOT NULL
        ORDER BY year
    """).df()
    if not df_growth.empty:
        st.bar_chart(df_growth, x="year", y="participants_count")

else:
    # ── Single edition view ─────────────────────────────────────────────────
    ed = next(e for e in editions if e[1] == selected_year)
    t_id, year, location, participants_count, notes = ed

    st.subheader(f"WK {year} — {location or '?'}")
    info_cols = st.columns(2)
    info_cols[0].metric("Field size", participants_count if participants_count else "—")
    info_cols[1].metric("Location", location or "—")
    if notes:
        st.caption(notes)

    df_part = conn.execute(f"""
        SELECT
            tp.final_rank AS "Rank",
            {DISPLAY_NAME} AS "Player",
            p.country AS "Country",
            tp.note AS "Note"
        FROM tournament_participants tp
        JOIN players p ON p.id = tp.player_id
        {ALIAS_JOIN}
        WHERE tp.tournament_id = ?
        ORDER BY tp.final_rank NULLS LAST
    """, [t_id]).df()
    st.dataframe(df_part, use_container_width=True, hide_index=True)

conn.close()

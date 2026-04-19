"""BCLC (Belgian Carcassonne Live Championship) tournament overview."""
from pathlib import Path

import duckdb
import streamlit as st

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"

STAGE_ORDER = {
    "Final": 1,
    "3rd Place": 2,
    "5th Place": 3,
    "7th Place": 4,
    "9th Place": 5,
    "11th Place": 6,
    "13th Place": 7,
    "15th Place": 8,
    "1/2 Finals": 10,
    "5th-8th SF": 11,
    "9th-12th SF": 12,
    "13th-16th SF": 13,
    "1/4 Finals": 20,
    "Loser Bracket R1": 21,
    "1/8 Finals": 30,
    "Round 6": 46,
    "Round 5": 45,
    "Round 4": 44,
    "Round 3": 43,
    "Round 2": 42,
    "Round 1": 41,
}

STAGE_ORDER_SQL = " ".join(
    f"WHEN '{s}' THEN {v}" for s, v in STAGE_ORDER.items()
)

st.title("BCLC Tournaments")

if not DB_PATH.exists():
    st.warning("No database found.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Filters ──────────────────────────────────────────────────────────────────

col_year, col_stage = st.columns([1, 1])

with col_year:
    years = conn.execute("""
        SELECT DISTINCT t.year
        FROM tournaments t
        WHERE t.type = 'BCLC'
        ORDER BY t.year DESC
    """).fetchall()
    year_options = ["All years"] + [r[0] for r in years]
    if len(year_options) == 1:
        st.info("No BCLC tournaments found.")
        conn.close()
        st.stop()
    selected_year = st.selectbox("Year", year_options)

if selected_year == "All years":
    bclc_tournaments = conn.execute(
        "SELECT id, name, year FROM tournaments WHERE type = 'BCLC' ORDER BY year DESC"
    ).fetchall()
    tourn_ids = [r[0] for r in bclc_tournaments]
    tournament_label = f"BCLC All years ({len(tourn_ids)})"
else:
    tournament = conn.execute(
        "SELECT id, name FROM tournaments WHERE type = 'BCLC' AND year = ?",
        [selected_year],
    ).fetchone()
    if not tournament:
        st.info(f"No BCLC tournament found for {selected_year}.")
        conn.close()
        st.stop()
    tourn_ids = [tournament[0]]
    tournament_label = tournament[1]

tourn_placeholders = ",".join(["?"] * len(tourn_ids))

# Stage list = playoff stages (from tournament_matches) + "Round N" for Swiss games
with col_stage:
    playoff_stages = [r[0] for r in conn.execute(f"""
        SELECT DISTINCT stage FROM tournament_matches
        WHERE tournament_id IN ({tourn_placeholders})
    """, tourn_ids).fetchall()]

    swiss_rounds = [r[0] for r in conn.execute(f"""
        SELECT DISTINCT round FROM games
        WHERE tournament_id IN ({tourn_placeholders})
          AND source = 'swiss'
        ORDER BY round
    """, tourn_ids).fetchall()]
    swiss_stages = [f"Round {r}" for r in swiss_rounds]

    all_stages = sorted(
        playoff_stages + swiss_stages,
        key=lambda s: STAGE_ORDER.get(s, 99),
    )
    stage_options = ["All stages"] + all_stages
    selected_stage = st.selectbox("Stage", stage_options)

# ── Build stage filter as SQL fragment on games ──────────────────────────────

where_game_clauses = [f"g.tournament_id IN ({tourn_placeholders})"]
game_params = list(tourn_ids)

if selected_stage != "All stages":
    if selected_stage.startswith("Round "):
        rnum = int(selected_stage.split()[1])
        where_game_clauses.append("g.source = 'swiss' AND g.round = ?")
        game_params.append(rnum)
    else:
        where_game_clauses.append("tm.stage = ?")
        game_params.append(selected_stage)

where_game_sql = "WHERE " + " AND ".join(where_game_clauses)

# ── Summary metrics ──────────────────────────────────────────────────────────

game_count = conn.execute(f"""
    SELECT COUNT(*) FROM games g
    LEFT JOIN tournament_matches tm
        ON tm.game_id_1 = g.id AND tm.tournament_id = g.tournament_id
    {where_game_sql}
""", game_params).fetchone()[0]

participant_count = conn.execute(f"""
    SELECT COUNT(*) FROM tournament_participants
    WHERE tournament_id IN ({tourn_placeholders})
""", tourn_ids).fetchone()[0]

playoff_count = conn.execute(f"""
    SELECT COUNT(*) FROM tournament_matches
    WHERE tournament_id IN ({tourn_placeholders})
""", tourn_ids).fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Tournament", tournament_label)
c2.metric("Participants", participant_count)
c3.metric("Games", game_count)
c4.metric("Playoff matches", playoff_count)

# ── Final ranking ────────────────────────────────────────────────────────────

if selected_stage == "All stages":
    st.subheader("Final ranking")

    if selected_year == "All years":
        bclc_years = [r[2] for r in bclc_tournaments]
        year_rank_cols = ",\n                ".join(
            f'MAX(CASE WHEN t.year = {y} THEN tp.final_rank END) AS "{y}"'
            for y in bclc_years
        )

        df_ranking = conn.execute(f"""
            SELECT
                CASE
                    WHEN p.name_nl IS NOT NULL AND p.national_team THEN p.name_nl || ' (' || p.name || ')'
                    WHEN p.name_nl IS NOT NULL THEN p.name_nl
                    ELSE p.name
                END AS "Player",
                COUNT(*) AS "Editions",
                SUM(CASE WHEN tp.final_rank = 1 THEN 1 ELSE 0 END) AS "Gold",
                SUM(CASE WHEN tp.final_rank = 2 THEN 1 ELSE 0 END) AS "Silver",
                SUM(CASE WHEN tp.final_rank = 3 THEN 1 ELSE 0 END) AS "Bronze",
                SUM(CASE WHEN tp.final_rank <= 16 THEN 1 ELSE 0 END) AS "Top16",
                MIN(tp.final_rank) AS "Best",
                ROUND(AVG(tp.final_rank), 1) AS "Avg rank",
                {year_rank_cols}
            FROM tournament_participants tp
            JOIN players p ON tp.player_id = p.id
            JOIN tournaments t ON tp.tournament_id = t.id
            WHERE tp.tournament_id IN ({tourn_placeholders})
            GROUP BY p.id, p.name, p.name_nl, p.national_team
            ORDER BY "Gold" DESC, "Silver" DESC, "Bronze" DESC, "Best" ASC, "Editions" DESC
        """, list(tourn_ids)).df()
    else:
        df_ranking = conn.execute("""
            SELECT
                tp.final_rank AS "Rank",
                CASE
                    WHEN p.name_nl IS NOT NULL AND p.national_team THEN p.name_nl || ' (' || p.name || ')'
                    WHEN p.name_nl IS NOT NULL THEN p.name_nl
                    ELSE p.name
                END AS "Player",
                tp.points AS "Points",
                ROUND(tp.resistance, 4) AS "Resistance"
            FROM tournament_participants tp
            JOIN players p ON tp.player_id = p.id
            WHERE tp.tournament_id = ?
            ORDER BY tp.final_rank
        """, [tourn_ids[0]]).df()

    if not df_ranking.empty:
        st.dataframe(df_ranking, use_container_width=True, hide_index=True)
    else:
        st.info("No ranking available for this tournament.")

# ── Player stats (all games: Swiss + Playoff) ────────────────────────────────

if game_count > 0:
    st.subheader("Player stats (all games)")

    df_players = conn.execute(f"""
        SELECT
            gp.player_id,
            CASE
                    WHEN p.name_nl IS NOT NULL AND p.national_team THEN p.name_nl || ' (' || p.name || ')'
                    WHEN p.name_nl IS NOT NULL THEN p.name_nl
                    ELSE p.name
                END AS "Player",
            COUNT(*) AS "Games",
            SUM(CASE WHEN gp.rank = 1 THEN 1 ELSE 0 END) AS "W",
            SUM(CASE WHEN gp.rank = 2 THEN 1 ELSE 0 END) AS "L",
            ROUND(100.0 * SUM(CASE WHEN gp.rank = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS "Win%",
            SUM(gp.score) AS "PF",
            ROUND(AVG(gp.score), 1) AS "Avg PF"
        FROM game_players gp
        JOIN games g ON gp.game_id = g.id
        LEFT JOIN tournament_matches tm
            ON tm.game_id_1 = g.id AND tm.tournament_id = g.tournament_id
        JOIN players p ON gp.player_id = p.id
        {where_game_sql}
        GROUP BY gp.player_id, p.name, p.name_nl, p.national_team
        ORDER BY "W" DESC, "PF" DESC
    """, game_params).df()

    if not df_players.empty:
        event = st.dataframe(
            df_players.drop(columns=["player_id"]),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="bclc_players_table",
        )

        if event.selection and event.selection.rows:
            selected_row = event.selection.rows[0]
            clicked_player_id = int(df_players.iloc[selected_row]["player_id"])
            clicked_player_name = df_players.iloc[selected_row]["Player"]
            st.session_state["bclc_detail_player_id"] = clicked_player_id
            st.session_state["bclc_detail_player_name"] = clicked_player_name

    # ── Player detail (all games) ────────────────────────────────────────────

    detail_player_id = st.session_state.get("bclc_detail_player_id")
    if detail_player_id:
        detail_name = st.session_state.get("bclc_detail_player_name", str(detail_player_id))

        st.subheader(f"All BCLC games — {detail_name}")

        if st.button("Clear player selection"):
            del st.session_state["bclc_detail_player_id"]
            if "bclc_detail_player_name" in st.session_state:
                del st.session_state["bclc_detail_player_name"]
            st.rerun()

        detail_params = list(game_params) + [detail_player_id]

        df_games = conn.execute(f"""
            SELECT
                t.year AS "Year",
                COALESCE(tm.stage, 'Round ' || g.round) AS "Stage",
                g.table_number AS "Table",
                me.score AS "For",
                CASE
                    WHEN opp_p.name_nl IS NOT NULL AND opp_p.national_team THEN opp_p.name_nl || ' (' || opp_p.name || ')'
                    WHEN opp_p.name_nl IS NOT NULL THEN opp_p.name_nl
                    ELSE opp_p.name
                END AS "Opponent",
                opp.score AS "Against",
                CASE WHEN me.rank = 1 THEN 'W' ELSE 'L' END AS "Result"
            FROM game_players me
            JOIN games g ON me.game_id = g.id
            JOIN tournaments t ON g.tournament_id = t.id
            JOIN game_players opp ON opp.game_id = g.id AND opp.player_id <> me.player_id
            JOIN players opp_p ON opp.player_id = opp_p.id
            LEFT JOIN tournament_matches tm
                ON tm.game_id_1 = g.id AND tm.tournament_id = g.tournament_id
            {where_game_sql.replace('g.tournament_id', 'g.tournament_id')}
              AND me.player_id = ?
            ORDER BY
                t.year DESC,
                CASE COALESCE(tm.stage, 'Round ' || g.round) {STAGE_ORDER_SQL} ELSE 99 END,
                g.table_number NULLS LAST
        """, detail_params).df()

        if not df_games.empty:
            st.dataframe(df_games, use_container_width=True, hide_index=True)
        else:
            st.info("No games found for this player with current filters.")

# ── All games ────────────────────────────────────────────────────────────────

if game_count > 0:
    st.subheader("All games")

    df_all = conn.execute(f"""
        WITH g_players AS (
            SELECT game_id,
                   MAX(CASE WHEN rank = 1 THEN player_id END) AS winner_id,
                   MAX(CASE WHEN rank = 1 THEN score END) AS winner_score,
                   MAX(CASE WHEN rank = 2 THEN player_id END) AS loser_id,
                   MAX(CASE WHEN rank = 2 THEN score END) AS loser_score
            FROM game_players
            GROUP BY game_id
        )
        SELECT
            t.year AS "Year",
            COALESCE(tm.stage, 'Round ' || g.round) AS "Stage",
            g.table_number AS "Table",
            CASE
                WHEN pw.name_nl IS NOT NULL AND pw.national_team THEN pw.name_nl || ' (' || pw.name || ')'
                WHEN pw.name_nl IS NOT NULL THEN pw.name_nl
                ELSE pw.name
            END AS "Winner",
            gp.winner_score AS "Score W",
            gp.loser_score AS "Score L",
            CASE
                WHEN pl.name_nl IS NOT NULL AND pl.national_team THEN pl.name_nl || ' (' || pl.name || ')'
                WHEN pl.name_nl IS NOT NULL THEN pl.name_nl
                ELSE pl.name
            END AS "Loser",
            tm.notes AS "Notes"
        FROM games g
        JOIN tournaments t ON g.tournament_id = t.id
        JOIN g_players gp ON gp.game_id = g.id
        LEFT JOIN players pw ON gp.winner_id = pw.id
        LEFT JOIN players pl ON gp.loser_id = pl.id
        LEFT JOIN tournament_matches tm
            ON tm.game_id_1 = g.id AND tm.tournament_id = g.tournament_id
        {where_game_sql}
        ORDER BY
            t.year DESC,
            CASE COALESCE(tm.stage, 'Round ' || g.round) {STAGE_ORDER_SQL} ELSE 99 END,
            g.table_number NULLS LAST
    """, game_params).df()

    if not df_all.empty:
        st.dataframe(df_all, use_container_width=True, hide_index=True)

conn.close()

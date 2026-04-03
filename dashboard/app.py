import streamlit as st

st.set_page_config(page_title="Carcassonne Belgium Stats", page_icon="🏰", layout="wide")

st.title("🏰 Carcassonne Belgium Stats")
st.caption("Statistieken van Belgische Carcassonne spelers")

st.page_link("pages/1_import.py", label="📥 BGA Data importeren", icon="📥")
st.page_link("pages/2_players.py", label="👤 Spelers", icon="👤")

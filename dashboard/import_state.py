"""
Singleton module voor import state.
Wordt éénmaal geladen per Python proces — niet gereset bij Streamlit reruns.
"""

state = {
    "log":     [],
    "running": False,
    "done":    False,
    "total":   0,
}

country_state = {
    "log":     [],
    "running": False,
    "done":    False,
    "updated": 0,
}

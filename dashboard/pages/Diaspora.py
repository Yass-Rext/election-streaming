"""Votes de la diaspora."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from database import load_table, show_empty_state, table_is_empty

st.title("Votes de la diaspora")

df = load_table("resultats_diaspora")

if table_is_empty(df):
    show_empty_state("Aucun vote diaspora pour le moment.")
else:
    st.dataframe(df.sort_values("nb_votes", ascending=False), use_container_width=True)

    fig = px.sunburst(
        df,
        path=["continent", "pays"],
        values="nb_votes",
        title="Répartition diaspora (continent → pays)",
    )
    st.plotly_chart(fig, use_container_width=True)

"""Votes par région."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from database import load_table, show_empty_state, table_is_empty

st.title("Votes par région")

df = load_table("resultats_regions")

if table_is_empty(df):
    show_empty_state()
else:
    df = df.sort_values("nb_votes", ascending=False)
    st.dataframe(df, use_container_width=True)

    fig = px.bar(
        df,
        x="region",
        y="nb_votes",
        color="region",
        text="nb_votes",
        title="Répartition des votes par région (Sénégal)",
    )
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

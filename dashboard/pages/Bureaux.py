"""Votes par bureau."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from database import load_table, show_empty_state, table_is_empty

st.title("Votes par bureau")

df = load_table("resultats_bureaux")

if table_is_empty(df):
    show_empty_state()
else:
    top = df.sort_values("nb_votes", ascending=False).head(20)
    st.dataframe(top, use_container_width=True)

    fig = px.bar(
        top,
        x="bureau",
        y="nb_votes",
        color="nb_votes",
        text="nb_votes",
        title="Top 20 des bureaux de vote",
    )
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

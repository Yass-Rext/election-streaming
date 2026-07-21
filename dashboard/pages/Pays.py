"""Répartition par pays (diaspora)."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from database import load_table, show_empty_state, table_is_empty

st.title("Votes par pays (diaspora)")

df = load_table("resultats_diaspora")

if table_is_empty(df):
    show_empty_state("Aucun vote diaspora / pays pour le moment.")
else:
    by_pays = (
        df.groupby("pays", as_index=False)["nb_votes"]
        .sum()
        .sort_values("nb_votes", ascending=False)
    )
    st.dataframe(by_pays, use_container_width=True)

    fig = px.bar(
        by_pays,
        x="pays",
        y="nb_votes",
        color="nb_votes",
        text="nb_votes",
        title="Votes diaspora par pays",
    )
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

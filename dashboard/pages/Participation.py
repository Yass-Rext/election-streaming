"""Participation (sexe et âge)."""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from database import load_table, show_empty_state, table_is_empty

st.title("Participation")

col1, col2 = st.columns(2)

with col1:
    sexe = load_table("participation_sexe")
    if table_is_empty(sexe):
        show_empty_state("Pas encore de données de participation par sexe.")
    else:
        fig = px.pie(
            sexe,
            names="sexe",
            values="nb_votes",
            title="Participation par sexe",
        )
        st.plotly_chart(fig, use_container_width=True)

with col2:
    age = load_table("participation_age")
    if table_is_empty(age):
        show_empty_state("Pas encore de données de participation par âge.")
    else:
        age = age.sort_values("tranche_age")
        fig = px.bar(
            age,
            x="tranche_age",
            y="nb_votes",
            color="tranche_age",
            title="Participation par tranche d'âge",
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.subheader("Votes par profession")
profession = load_table("resultats_profession")
if table_is_empty(profession):
    show_empty_state("Pas encore de données par profession.")
else:
    profession = profession.sort_values("nb_votes", ascending=False).head(15)
    fig_p = px.bar(
        profession,
        x="profession",
        y="nb_votes",
        color="nb_votes",
        title="Top professions",
    )
    st.plotly_chart(fig_p, use_container_width=True)

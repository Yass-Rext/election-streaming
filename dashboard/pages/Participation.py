import streamlit as st
import plotly.express as px

from database import load_table

st.title("👥 Participation")

col1, col2 = st.columns(2)

with col1:

    sexe = load_table("participation_sexe")

    fig = px.pie(
        sexe,
        names="sexe",
        values="nb_votes",
        title="Participation par sexe"
    )

    st.plotly_chart(fig, use_container_width=True)

with col2:

    age = load_table("participation_age")

    fig = px.bar(
        age,
        x="tranche_age",
        y="nb_votes",
        color="tranche_age",
        title="Participation par âge"
    )

    st.plotly_chart(fig, use_container_width=True)
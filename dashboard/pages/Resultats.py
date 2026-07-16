import streamlit as st
import plotly.express as px

from database import load_table

st.title("🏆 Résultats par candidat")

df = load_table("resultats_candidats")

st.dataframe(df, use_container_width=True)

fig = px.bar(
    df,
    x="candidat",
    y="nb_votes",
    color="candidat",
    text="nb_votes"
)

st.plotly_chart(fig, use_container_width=True)
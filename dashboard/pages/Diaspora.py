import streamlit as st
import plotly.express as px

from database import load_table

st.title("🌍 Votes de la diaspora")

df = load_table("resultats_diaspora")

st.dataframe(df, use_container_width=True)

fig = px.sunburst(
    df,
    path=["continent", "pays"],
    values="nb_votes"
)

st.plotly_chart(fig, use_container_width=True)
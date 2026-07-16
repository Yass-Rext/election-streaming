import streamlit as st
import plotly.express as px

from database import load_table

st.title("🗳️ Votes par bureau")

df = load_table("resultats_bureaux")

st.dataframe(df, use_container_width=True)

fig = px.bar(
    df.sort_values("nb_votes", ascending=False).head(20),
    x="bureau",
    y="nb_votes",
    color="nb_votes",
    text="nb_votes"
)

st.plotly_chart(fig, use_container_width=True)
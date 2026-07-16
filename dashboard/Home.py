import streamlit as st

st.set_page_config(
    page_title="Election Streaming",
    page_icon="🗳️",
    layout="wide"
)

st.title("🗳️ Election Streaming Dashboard")

st.markdown("""
Pipeline :

Kafka → Spark → PostgreSQL → Streamlit
""")
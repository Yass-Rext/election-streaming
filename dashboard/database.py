import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "election")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(DATABASE_URL)
def load_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)


@st.cache_data(ttl=5)
def load_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)
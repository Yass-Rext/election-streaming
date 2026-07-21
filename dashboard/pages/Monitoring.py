"""Monitoring technique du pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st

from database import get_engine, load_table, table_is_empty

st.title("Monitoring")

st.caption(f"Dernière actualisation UI : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

tables = [
    "votes_bruts",
    "resultats_candidats",
    "resultats_regions",
    "resultats_departements",
    "resultats_bureaux",
    "resultats_diaspora",
    "participation_sexe",
    "participation_age",
    "resultats_profession",
]

rows = []
for table in tables:
    df = load_table(table)
    rows.append(
        {
            "table": table,
            "lignes": 0 if table_is_empty(df) else len(df),
            "statut": "OK" if not table_is_empty(df) else "vide",
        }
    )

st.subheader("État des tables PostgreSQL")
st.dataframe(rows, use_container_width=True)

st.subheader("Santé de la connexion")
try:
    engine = get_engine()
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
    st.success("Connexion PostgreSQL opérationnelle")
except Exception as exc:
    st.error(f"Connexion PostgreSQL en échec : {exc}")

votes = load_table("votes_bruts")
if not table_is_empty(votes) and "timestamp" in votes.columns:
    st.subheader("Activité récente")
    recent = votes.sort_values("timestamp", ascending=False).head(20)
    st.dataframe(recent, use_container_width=True)
else:
    st.info("Pas encore de votes bruts en base.")

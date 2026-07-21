"""Page d'accueil du dashboard Election Streaming."""

from __future__ import annotations

import streamlit as st

from database import load_table, table_is_empty

st.set_page_config(
    page_title="Election Streaming",
    page_icon="🗳️",
    layout="wide",
)

st.title("Election Streaming Dashboard")
st.markdown(
    """
Pipeline temps réel :

**Producer → Kafka → Spark Structured Streaming → PostgreSQL / MinIO → Streamlit**
"""
)

st.subheader("Vue d'ensemble")

col1, col2, col3, col4 = st.columns(4)

candidats = load_table("resultats_candidats")
regions = load_table("resultats_regions")
diaspora = load_table("resultats_diaspora")
sexe = load_table("participation_sexe")

total_votes = int(candidats["nb_votes"].sum()) if not table_is_empty(candidats) else 0
nb_candidats = len(candidats) if not table_is_empty(candidats) else 0
nb_regions = len(regions) if not table_is_empty(regions) else 0
nb_diaspora = int(diaspora["nb_votes"].sum()) if not table_is_empty(diaspora) else 0

col1.metric("Votes totaux", f"{total_votes:,}".replace(",", " "))
col2.metric("Candidats", nb_candidats)
col3.metric("Régions actives", nb_regions)
col4.metric("Votes diaspora", f"{nb_diaspora:,}".replace(",", " "))

if total_votes == 0:
    st.warning(
        "Aucune agrégation disponible. Vérifiez que Kafka, le producer et Spark "
        "sont démarrés (`docker compose ps`)."
    )
else:
    st.success("Pipeline actif — données rafraîchies automatiquement (cache 5s).")

st.markdown("---")
st.markdown(
    """
### Navigation

Utilisez le menu latéral pour consulter :

- Résultats par candidat
- Votes par région / département / bureau
- Diaspora et pays
- Participation (sexe, âge, profession)
- Monitoring technique
"""
)

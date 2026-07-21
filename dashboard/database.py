"""Accès PostgreSQL pour le dashboard Streamlit."""

from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "election")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


@st.cache_resource
def get_engine() -> Engine:
    """Crée (une seule fois) le moteur SQLAlchemy."""
    logger.info("Connexion PostgreSQL -> %s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


@st.cache_data(ttl=5)
def load_table(table_name: str) -> pd.DataFrame:
    """Charge une table PostgreSQL.

    Retourne un DataFrame vide (sans exception) si la table est absente
    ou si la connexion échoue — le dashboard ne doit jamais planter.
    """
    if not table_name.isidentifier():
        logger.error("Nom de table invalide: %s", table_name)
        return pd.DataFrame()

    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
        logger.info("Table %s chargée | rows=%s", table_name, len(df))
        return df
    except SQLAlchemyError:
        logger.exception("Erreur SQL lors du chargement de %s", table_name)
        return pd.DataFrame()
    except Exception:
        logger.exception("Erreur inattendue lors du chargement de %s", table_name)
        return pd.DataFrame()


def table_is_empty(df: Optional[pd.DataFrame]) -> bool:
    """Indique si un DataFrame est None ou vide."""
    return df is None or df.empty


def show_empty_state(message: str = "Aucune donnée disponible pour le moment.") -> None:
    """Affiche un message d'état vide cohérent."""
    st.info(message)
    st.caption("Les agrégations apparaîtront dès que Spark aura traité les premiers votes.")

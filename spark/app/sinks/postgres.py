"""Sink PostgreSQL (JDBC + SQL) pour Spark Structured Streaming."""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import psycopg2
from pyspark.sql import DataFrame

from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_URL,
    POSTGRES_USER,
)

logger = logging.getLogger(__name__)

# Recalcul côté Postgres : évite de relire toute la table via Spark à chaque batch.
AGGREGATION_SQL = (
    """
    TRUNCATE resultats_candidats;
    INSERT INTO resultats_candidats (candidat, nb_votes)
    SELECT candidat, COUNT(*) FROM votes_bruts
    WHERE candidat IS NOT NULL
    GROUP BY candidat;
    """,
    """
    TRUNCATE resultats_regions;
    INSERT INTO resultats_regions (region, nb_votes)
    SELECT region, COUNT(*) FROM votes_bruts
    WHERE type = 'SENEGAL' AND region IS NOT NULL
    GROUP BY region;
    """,
    """
    TRUNCATE resultats_departements;
    INSERT INTO resultats_departements (region, departement, nb_votes)
    SELECT region, departement, COUNT(*) FROM votes_bruts
    WHERE type = 'SENEGAL' AND departement IS NOT NULL
    GROUP BY region, departement;
    """,
    """
    TRUNCATE resultats_bureaux;
    INSERT INTO resultats_bureaux (bureau, nb_votes)
    SELECT bureau, COUNT(*) FROM votes_bruts
    WHERE bureau IS NOT NULL
    GROUP BY bureau;
    """,
    """
    TRUNCATE resultats_diaspora;
    INSERT INTO resultats_diaspora (continent, pays, nb_votes)
    SELECT continent, pays, COUNT(*) FROM votes_bruts
    WHERE type = 'DIASPORA' AND continent IS NOT NULL AND pays IS NOT NULL
    GROUP BY continent, pays;
    """,
    """
    TRUNCATE participation_sexe;
    INSERT INTO participation_sexe (sexe, nb_votes)
    SELECT sexe, COUNT(*) FROM votes_bruts
    WHERE sexe IS NOT NULL
    GROUP BY sexe;
    """,
    """
    TRUNCATE participation_age;
    INSERT INTO participation_age (tranche_age, nb_votes)
    SELECT tranche_age, COUNT(*) FROM votes_bruts
    WHERE tranche_age IS NOT NULL
    GROUP BY tranche_age;
    """,
    """
    TRUNCATE resultats_profession;
    INSERT INTO resultats_profession (profession, nb_votes)
    SELECT profession, COUNT(*) FROM votes_bruts
    WHERE profession IS NOT NULL
    GROUP BY profession;
    """,
)


def write_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    truncate: bool = True,
    row_count: Optional[int] = None,
) -> None:
    """Écrit un DataFrame dans une table PostgreSQL via JDBC."""
    started = time.perf_counter()

    writer = (
        df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("batchsize", "1000")
        .option("isolationLevel", "READ_COMMITTED")
        .mode(mode)
    )

    if mode == "overwrite" and truncate:
        writer = writer.option("truncate", "true")

    try:
        writer.save()
        elapsed = time.perf_counter() - started
        rows_info = row_count if row_count is not None else "?"
        logger.info(
            "PostgreSQL OK | table=%s | mode=%s | rows=%s | durée=%.2fs",
            table_name,
            mode,
            rows_info,
            elapsed,
        )
    except Exception:
        logger.exception(
            "Échec écriture PostgreSQL | table=%s | mode=%s",
            table_name,
            mode,
        )
        raise


def write_batch(table_name: str) -> Callable[[DataFrame, int], None]:
    """Factory foreachBatch : overwrite d'une table d'agrégation."""

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        logger.info("Début batch PostgreSQL | id=%s | table=%s", batch_id, table_name)
        try:
            if batch_df.rdd.isEmpty():
                logger.info(
                    "Batch vide — skip | id=%s | table=%s", batch_id, table_name
                )
                return
            write_postgres(batch_df, table_name, mode="overwrite", truncate=True)
            logger.info("Fin batch PostgreSQL | id=%s | table=%s", batch_id, table_name)
        except Exception:
            logger.exception(
                "Erreur batch PostgreSQL | id=%s | table=%s", batch_id, table_name
            )
            raise

    return _write


def append_votes(df: DataFrame, table_name: str = "votes_bruts") -> None:
    """Append des votes bruts dans PostgreSQL."""
    write_postgres(df, table_name, mode="append", truncate=False)


def refresh_aggregations_sql() -> None:
    """Recalcule toutes les tables d'agrégation directement en SQL PostgreSQL."""
    started = time.perf_counter()
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            for statement in AGGREGATION_SQL:
                cur.execute(statement)
            cur.execute("SELECT COUNT(*) FROM votes_bruts")
            total = cur.fetchone()[0]
        conn.commit()
        elapsed = time.perf_counter() - started
        logger.info(
            "Agrégations SQL OK | votes_bruts=%s | durée=%.2fs",
            total,
            elapsed,
        )
    except Exception:
        if conn is not None:
            conn.rollback()
        logger.exception("Échec du recalcul SQL des agrégations")
        raise
    finally:
        if conn is not None:
            conn.close()

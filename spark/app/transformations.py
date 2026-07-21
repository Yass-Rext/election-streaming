"""Transformations du flux de votes Kafka."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
    when,
)

from schemas import vote_schema

logger = logging.getLogger(__name__)


def parse_votes(df: DataFrame) -> DataFrame:
    """Convertit le JSON Kafka (colonne value) en colonnes typées."""
    return (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), vote_schema).alias("vote"))
        .select("vote.*")
    )


def clean_votes(df: DataFrame) -> DataFrame:
    """Nettoie et valide les votes parsés."""
    return (
        df.withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingestion_time", current_timestamp())
        .withColumn(
            "sexe",
            when(col("sexe").isin("H", "h", "M", "m"), "M")
            .when(col("sexe").isin("F", "f"), "F")
            .otherwise(col("sexe")),
        )
        .filter(col("age").isNotNull() & (col("age") >= 18) & (col("age") <= 120))
        .filter(col("candidat").isNotNull() & (col("candidat") != ""))
        .filter(col("vote_id").isNotNull() & (col("vote_id") != ""))
        .filter(col("bureau").isNotNull() & (col("bureau") != ""))
        .filter(col("type").isin("SENEGAL", "DIASPORA"))
    )


def enrich_votes(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes dérivées (diaspora, tranche d'âge)."""
    return df.withColumn(
        "est_diaspora",
        when(col("type") == "DIASPORA", True).otherwise(False),
    ).withColumn(
        "tranche_age",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 60, "45-59")
        .otherwise("60+"),
    )


def transform(df: DataFrame) -> DataFrame:
    """Pipeline complet : parse → clean → enrich."""
    logger.info("Application du pipeline de transformation")
    parsed = parse_votes(df)
    cleaned = clean_votes(parsed)
    return enrich_votes(cleaned)

"""Agrégations électorales sur le DataFrame de votes."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count


def votes_par_candidat(df: DataFrame) -> DataFrame:
    """Compte les votes par candidat."""
    return df.groupBy("candidat").agg(count("*").alias("nb_votes"))


def votes_par_region(df: DataFrame) -> DataFrame:
    """Compte les votes par région (Sénégal uniquement)."""
    return (
        df.filter(col("type") == "SENEGAL")
        .groupBy("region")
        .agg(count("*").alias("nb_votes"))
    )


def votes_par_departement(df: DataFrame) -> DataFrame:
    """Compte les votes par département (Sénégal uniquement)."""
    return (
        df.filter(col("type") == "SENEGAL")
        .groupBy("region", "departement")
        .agg(count("*").alias("nb_votes"))
    )


def votes_par_bureau(df: DataFrame) -> DataFrame:
    """Compte les votes par bureau."""
    return df.groupBy("bureau").agg(count("*").alias("nb_votes"))


def votes_diaspora(df: DataFrame) -> DataFrame:
    """Compte les votes diaspora par continent et pays."""
    return (
        df.filter(col("type") == "DIASPORA")
        .groupBy("continent", "pays")
        .agg(count("*").alias("nb_votes"))
    )


def participation_sexe(df: DataFrame) -> DataFrame:
    """Participation par sexe."""
    return df.groupBy("sexe").agg(count("*").alias("nb_votes"))


def participation_age(df: DataFrame) -> DataFrame:
    """Participation par tranche d'âge."""
    return df.groupBy("tranche_age").agg(count("*").alias("nb_votes"))


def votes_profession(df: DataFrame) -> DataFrame:
    """Votes par profession."""
    return (
        df.filter(col("profession").isNotNull())
        .groupBy("profession")
        .agg(count("*").alias("nb_votes"))
    )

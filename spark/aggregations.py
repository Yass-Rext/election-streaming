from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    count,
    window,
    col
)


# ===========================================
# Résultats par candidat
# ===========================================

def votes_par_candidat(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("candidat")
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy(col("nb_votes").desc())
    )


# ===========================================
# Résultats par région
# ===========================================

def votes_par_region(df: DataFrame) -> DataFrame:

    return (
        df.filter(col("type") == "SENEGAL")
        .groupBy("region")
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy(col("nb_votes").desc())
    )


# ===========================================
# Résultats par département
# ===========================================

def votes_par_departement(df: DataFrame) -> DataFrame:

    return (
        df.filter(col("type") == "SENEGAL")
        .groupBy(
            "region",
            "departement"
        )
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy(col("nb_votes").desc())
    )


# ===========================================
# Résultats par bureau
# ===========================================

def votes_par_bureau(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("bureau")
        .agg(
            count("*").alias("nb_votes")
        )
    )


# ===========================================
# Votes Diaspora
# ===========================================

def votes_diaspora(df: DataFrame) -> DataFrame:

    return (
        df.filter(col("type") == "DIASPORA")
        .groupBy(
            "continent",
            "pays"
        )
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy(col("nb_votes").desc())
    )


# ===========================================
# Participation par sexe
# ===========================================

def participation_sexe(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("sexe")
        .agg(
            count("*").alias("nb_votes")
        )
    )


# ===========================================
# Participation par tranche d'âge
# ===========================================

def participation_age(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("tranche_age")
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy("tranche_age")
    )


# ===========================================
# Votes par profession
# ===========================================

def votes_profession(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("profession")
        .agg(
            count("*").alias("nb_votes")
        )
        .orderBy(col("nb_votes").desc())
    )


# ===========================================
# Evolution des votes
# Fenêtre glissante de 1 minute
# ===========================================

def evolution_votes(df: DataFrame) -> DataFrame:

    return (
        df
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            window(
                col("timestamp"),
                "1 minute"
            )
        )
        .agg(
            count("*").alias("nb_votes")
        )
    )
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    when
)

from schemas import vote_schema


def parse_votes(df: DataFrame) -> DataFrame:
    """
    Convertit le JSON Kafka en DataFrame Spark.
    """

    return (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), vote_schema).alias("vote"))
        .select("vote.*")
    )


def clean_votes(df: DataFrame) -> DataFrame:
    """
    Nettoyage des données.
    """

    return (
        df

        # timestamp -> TimestampType
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"))
        )

        # timestamp d'ingestion Spark
        .withColumn(
            "ingestion_time",
            current_timestamp()
        )

        # Normalisation du sexe
        .withColumn(
            "sexe",
            when(col("sexe") == "H", "M")
            .otherwise(col("sexe"))
        )

        # Age valide
        .filter(
            (col("age") >= 18)
            &
            (col("age") <= 120)
        )

        # candidat obligatoire
        .filter(
            col("candidat").isNotNull()
        )

        # vote_id obligatoire
        .filter(
            col("vote_id").isNotNull()
        )

        # bureau obligatoire
        .filter(
            col("bureau").isNotNull()
        )
    )


def enrich_votes(df: DataFrame) -> DataFrame:
    """
    Colonnes calculées.
    """

    return (

        df

        .withColumn(
            "est_diaspora",
            when(col("type") == "DIASPORA", True)
            .otherwise(False)
        )

        .withColumn(
            "tranche_age",

            when(col("age") < 25, "18-24")
            .when(col("age") < 35, "25-34")
            .when(col("age") < 45, "35-44")
            .when(col("age") < 60, "45-59")
            .otherwise("60+")
        )

    )


def transform(df: DataFrame) -> DataFrame:
    """
    Pipeline complet.
    """

    df = parse_votes(df)

    df = clean_votes(df)

    df = enrich_votes(df)

    return df
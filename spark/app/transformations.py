from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    when,
    coalesce,
    lit
)

from schemas import vote_schema


# =====================================================
# Lecture JSON Kafka
# =====================================================

def parse_votes(df: DataFrame) -> DataFrame:

    return (
        df.selectExpr("CAST(value AS STRING) AS json")
        .select(
            from_json(col("json"), vote_schema).alias("vote")
        )
        .select("vote.*")
    )


# =====================================================
# Nettoyage
# =====================================================

def clean_votes(df: DataFrame) -> DataFrame:

    return (

        df

        .withColumn(
            "timestamp",
            coalesce(
                to_timestamp(col("timestamp")),
                current_timestamp()
            )
        )

        .withColumn(
            "ingestion_time",
            current_timestamp()
        )

        .withColumn(
            "profession",
            coalesce(
                col("profession"),
                lit("Inconnue")
            )
        )

        .withColumn(
            "region",
            coalesce(
                col("region"),
                lit("DIASPORA")
            )
        )

        .withColumn(
            "departement",
            coalesce(
                col("departement"),
                lit("DIASPORA")
            )
        )

        .withColumn(
            "centre",
            coalesce(
                col("centre"),
                lit("DIASPORA")
            )
        )

        .withColumn(
            "continent",
            coalesce(
                col("continent"),
                lit("")
            )
        )

        .withColumn(
            "pays",
            coalesce(
                col("pays"),
                lit("")
            )
        )

        .withColumn(
            "ville",
            coalesce(
                col("ville"),
                lit("")
            )
        )

        .withColumn(
            "bureau",
            coalesce(
                col("bureau"),
                lit("UNKNOWN")
            )
        )

        .withColumn(
            "candidat",
            coalesce(
                col("candidat"),
                lit("UNKNOWN")
            )
        )

        .withColumn(
            "age",
            coalesce(
                col("age"),
                lit(18)
            )
        )

        .filter(
            (col("age") >= 18) &
            (col("age") <= 120)
        )

        .withColumn(
            "sexe",
            when(col("sexe") == "H", "M")
            .otherwise(col("sexe"))
        )
    )


# =====================================================
# Enrichissement
# =====================================================

def enrich_votes(df: DataFrame) -> DataFrame:

    return (

        df

        .withColumn(
            "est_diaspora",
            when(
                col("type") == "DIASPORA",
                True
            ).otherwise(False)
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


# =====================================================
# Pipeline
# =====================================================

def transform(df: DataFrame) -> DataFrame:

    df = parse_votes(df)
    df = clean_votes(df)
    df = enrich_votes(df)

    return df
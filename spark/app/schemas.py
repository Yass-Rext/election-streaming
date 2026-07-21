"""Schéma Spark des messages de vote (compatible Producer)."""

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

vote_schema = StructType(
    [
        StructField("vote_id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("cni", StringType(), nullable=True),
        StructField("nom", StringType(), nullable=True),
        StructField("prenom", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("sexe", StringType(), nullable=True),
        StructField("profession", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True),
        StructField("departement", StringType(), nullable=True),
        StructField("centre", StringType(), nullable=True),
        StructField("bureau", StringType(), nullable=True),
        StructField("continent", StringType(), nullable=True),
        StructField("pays", StringType(), nullable=True),
        StructField("ville", StringType(), nullable=True),
        StructField("candidat", StringType(), nullable=False),
    ]
)

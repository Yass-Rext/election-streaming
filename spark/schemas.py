from pyspark.sql.types import *

vote_schema = StructType([

    StructField("vote_id", StringType()),

    StructField("timestamp", StringType()),

    StructField("type", StringType()),

    StructField("cni", StringType()),

    StructField("nom", StringType()),

    StructField("prenom", StringType()),

    StructField("age", IntegerType()),

    StructField("sexe", StringType()),

    StructField("profession", StringType()),

    StructField("region", StringType()),

    StructField("departement", StringType()),

    StructField("centre", StringType()),

    StructField("bureau", StringType()),

    StructField("continent", StringType()),

    StructField("pays", StringType()),

    StructField("ville", StringType()),

    StructField("candidat", StringType())

])
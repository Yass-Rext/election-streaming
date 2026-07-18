from pyspark.sql.types import *

vote_schema = StructType([

    StructField("vote_id", StringType(), True),

    StructField("timestamp", StringType(), True),

    StructField("type", StringType(), True),

    StructField("cni", StringType(), True),

    StructField("nom", StringType(), True),

    StructField("prenom", StringType(), True),

    StructField("age", IntegerType(), True),

    StructField("sexe", StringType(), True),

    # Sénégal
    StructField("profession", StringType(), True),
    StructField("region", StringType(), True),
    StructField("departement", StringType(), True),
    StructField("centre", StringType(), True),

    # Commun
    StructField("bureau", StringType(), True),

    # Diaspora
    StructField("zone", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("pays", StringType(), True),
    StructField("ville", StringType(), True),

    # Vote
    StructField("candidat", StringType(), True)

])
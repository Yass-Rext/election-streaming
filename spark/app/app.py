from pyspark.sql import SparkSession
from pyspark import StorageLevel

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)

from transformations import transform

from aggregations import (
    votes_par_candidat,
    votes_par_region,
    votes_par_departement,
    votes_par_bureau,
    votes_diaspora,
    participation_sexe,
    participation_age,
    votes_profession,
)

from sinks.postgres import write_batch
from sinks.minio import configure_minio
from sinks.minio import write_batch as write_minio_batch


# ==========================================================
# Spark Session
# ==========================================================

spark = (
    SparkSession.builder
    .appName("ElectionStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

configure_minio(spark)

print("=" * 60)
print("SPARK SESSION OK")
print("=" * 60)

# ==========================================================
# Lecture Kafka
# ==========================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

print("=" * 60)
print("KAFKA CONNECTED")
print("=" * 60)

# ==========================================================
# Transformations
# ==========================================================

votes = (
    transform(raw_stream)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("=" * 60)
print("TRANSFORMATIONS OK")
print("=" * 60)

# ==========================================================
# Sauvegarde MinIO
# ==========================================================

raw_query = (
    votes.writeStream
    .foreachBatch(write_minio_batch())
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/app/checkpoints/minio"
    )
    .start()
)

# ==========================================================
# Fonction générique
# ==========================================================

def start_query(df, table, checkpoint):

    return (
        df.writeStream
        .outputMode("complete")
        .trigger(processingTime="5 seconds")
        .foreachBatch(write_batch(table))
        .option(
            "checkpointLocation",
            f"/app/checkpoints/{checkpoint}"
        )
        .start()
    )

# ==========================================================
# Lancement des streams
# ==========================================================

queries = [

    start_query(
        votes_par_candidat(votes),
        "resultats_candidats",
        "candidats"
    ),

    start_query(
        votes_par_region(votes),
        "resultats_regions",
        "regions"
    ),

    start_query(
        votes_par_departement(votes),
        "resultats_departements",
        "departements"
    ),

    start_query(
        votes_par_bureau(votes),
        "resultats_bureaux",
        "bureaux"
    ),

    start_query(
        votes_diaspora(votes),
        "resultats_diaspora",
        "diaspora"
    ),

    start_query(
        participation_sexe(votes),
        "participation_sexe",
        "sexe"
    ),

    start_query(
        participation_age(votes),
        "participation_age",
        "age"
    ),

    start_query(
        votes_profession(votes),
        "resultats_profession",
        "profession"
    ),
]

print("=" * 60)
print("ELECTION STREAMING STARTED")
print("=" * 60)

spark.streams.awaitAnyTermination()
from pyspark.sql import SparkSession

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
    votes_profession
)

from sinks.postgres import write_batch
from sinks.minio import configure_minio

from sinks.minio import write_batch as write_minio_batch
from pyspark import StorageLevel


# ======================================
# Spark Session
# ======================================

spark = (
    SparkSession.builder
    .appName("Election Streaming")

    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    )

    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

configure_minio(spark)

print("Spark démarré...")


# ======================================
# Lecture Kafka
# ======================================

raw_stream = (

    spark.readStream

    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )

    .option(
        "subscribe",
        KAFKA_TOPIC
    )

    .option(
        "startingOffsets",
        "earliest"
    )

    .load()

)

print("Connexion Kafka OK")


# ======================================
# Transformations
# ======================================

# votes = transform(raw_stream)


votes = (
    transform(raw_stream)
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Transformation OK")


# ======================================
# Sauvegarde brute (console)
# (MinIO ensuite)
# ======================================



raw_query = (
    votes
    .writeStream
    .foreachBatch(
        write_minio_batch()
    )
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/minio"
    )
    .start()
)

# ======================================
# Agrégation candidat
# ======================================

def start_query(df, table, checkpoint):

    return (
        df.writeStream
        .trigger(processingTime="5 seconds")
        .outputMode("complete")
        .foreachBatch(
            write_batch(table)
        )
        .option(
            "checkpointLocation",
            checkpoint
        )
        .start()
    )




queries = []

queries.append(
    start_query(
        votes_par_candidat(votes),
        "resultats_candidats",
        "checkpoints/candidats"
    )
)

# ======================================
# Agrégation région
# ======================================

queries.append(
    start_query(
        votes_par_region(votes),
        "resultats_regions",
        "checkpoints/regions"
    )
)

# ======================================
# Agrégation département
# ======================================

queries.append(
    start_query(
        votes_par_departement(votes),
        "resultats_departements",
        "checkpoints/departements"
    )
)


# ======================================
# Agrégation bureaux
# ======================================

queries.append(
    start_query(
        votes_par_bureau(votes),
        "resultats_bureaux",
        "checkpoints/bureaux"
    )
)


# ======================================
# Diaspora
# ======================================

queries.append(
    start_query(
        votes_diaspora(votes),
        "resultats_diaspora",
        "checkpoints/diaspora"
    )
)

# ======================================
# Sexe
# ======================================

queries.append(
    start_query(
        participation_sexe(votes),
        "participation_sexe",
        "checkpoints/sexe"
    )
)


# ======================================
# Age
# ======================================

queries.append(
    start_query(
        participation_age(votes),
        "participation_age",
        "checkpoints/age"
    )
)


# ======================================
# Profession
# ======================================

queries.append(
    start_query(
        votes_profession(votes),
        "resultats_profession",
        "checkpoints/profession"
    )
)


print("=" * 60)
print("Election Streaming lancé")
print("=" * 60)

try:
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("Arrêt du streaming...")

finally:
    spark.stop()
"""
Application Spark Structured Streaming — Election Streaming.

Pipeline :
  Kafka (votes) → parse/clean/enrich → foreachBatch
    → MinIO (Parquet append)
    → PostgreSQL votes_bruts (append)
    → recalcul SQL des tables d'agrégation (rapide, côté Postgres)
"""

from __future__ import annotations

import logging
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from config import (
    CHECKPOINT_BASE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_STARTING_OFFSETS,
    KAFKA_TOPIC,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SHUFFLE_PARTITIONS,
    TRIGGER_INTERVAL,
)
from constants import VOTES_BRUTS
from sinks.minio import configure_minio, write_minio
from sinks.postgres import append_votes, refresh_aggregations_sql
from transformations import transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("spark-app")

SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.7.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


def create_spark_session() -> SparkSession:
    """Crée et configure la SparkSession."""
    spark = (
        SparkSession.builder.appName("ElectionStreaming")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Ouvre le flux Kafka sur le topic des votes."""
    logger.info(
        "Connexion Kafka | bootstrap=%s | topic=%s | offsets=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_STARTING_OFFSETS,
    )
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Traite un micro-batch : MinIO + Postgres bruts + agrégations SQL."""
    started = time.perf_counter()
    logger.info("========== Début batch %s ==========", batch_id)

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if batch_df.rdd.isEmpty():
            logger.info("Batch %s vide — rien à écrire", batch_id)
            return

        row_count = batch_df.count()
        logger.info("Batch %s | messages lus (après transform)=%s", batch_id, row_count)

        write_minio(batch_df, row_count=row_count)
        append_votes(batch_df, VOTES_BRUTS)
        refresh_aggregations_sql()

        elapsed = time.perf_counter() - started
        logger.info(
            "========== Fin batch %s | rows=%s | durée=%.2fs ==========",
            batch_id,
            row_count,
            elapsed,
        )
    except Exception:
        logger.exception("Erreur fatale dans le batch %s", batch_id)
        raise
    finally:
        batch_df.unpersist()


def main() -> None:
    """Point d'entrée du job de streaming."""
    logger.info(
        "Config Postgres | host=%s port=%s db=%s user=%s",
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        POSTGRES_USER,
    )

    spark = create_spark_session()
    configure_minio(spark)
    logger.info("SparkSession démarrée")

    raw_stream = read_kafka_stream(spark)
    logger.info("Connexion Kafka OK")

    votes = transform(raw_stream)
    logger.info("Pipeline de transformation prêt")

    query = (
        votes.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
        .queryName("election_main_pipeline")
        .start()
    )

    logger.info("=" * 60)
    logger.info("Election Streaming lancé | trigger=%s", TRIGGER_INTERVAL)
    logger.info("=" * 60)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Arrêt demandé (KeyboardInterrupt)")
    finally:
        logger.info("Arrêt des streams et de la SparkSession...")
        for active in spark.streams.active:
            active.stop()
        spark.stop()
        logger.info("Spark arrêté proprement")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Arrêt fatal de l'application Spark")
        sys.exit(1)

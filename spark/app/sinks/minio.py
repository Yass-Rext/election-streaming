"""Sink MinIO (S3A / Parquet) pour Spark Structured Streaming."""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession

from config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logger = logging.getLogger(__name__)


def configure_minio(spark: SparkSession) -> None:
    """Configure le filesystem S3A pour MinIO."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    settings = {
        "fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
        "fs.s3a.access.key": MINIO_ACCESS_KEY,
        "fs.s3a.secret.key": MINIO_SECRET_KEY,
        "fs.s3a.path.style.access": "true",
        "fs.s3a.connection.ssl.enabled": "false",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "fs.s3a.fast.upload": "true",
        "fs.s3a.multipart.size": "67108864",
    }

    for key, value in settings.items():
        hadoop_conf.set(key, value)

    logger.info(
        "Connexion MinIO configurée | endpoint=%s | bucket=%s",
        MINIO_ENDPOINT,
        MINIO_BUCKET,
    )


def write_minio(
    df: DataFrame,
    path_suffix: str = "votes",
    row_count: Optional[int] = None,
) -> None:
    """Écrit un DataFrame en Parquet vers MinIO."""
    target = f"s3a://{MINIO_BUCKET}/{path_suffix}/"
    started = time.perf_counter()

    try:
        df.write.mode("append").option("compression", "snappy").parquet(target)
        elapsed = time.perf_counter() - started
        rows_info = row_count if row_count is not None else "?"
        logger.info(
            "MinIO OK | path=%s | rows=%s | durée=%.2fs",
            target,
            rows_info,
            elapsed,
        )
    except Exception:
        logger.exception("Échec écriture MinIO | path=%s", target)
        raise


def write_batch() -> Callable[[DataFrame, int], None]:
    """Factory foreachBatch : append Parquet vers MinIO."""

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        logger.info("Début batch MinIO | id=%s", batch_id)
        try:
            if batch_df.rdd.isEmpty():
                logger.info("Batch MinIO vide — skip | id=%s", batch_id)
                return
            write_minio(batch_df)
            logger.info("Fin batch MinIO | id=%s", batch_id)
        except Exception:
            logger.exception("Erreur batch MinIO | id=%s", batch_id)
            raise

    return _write

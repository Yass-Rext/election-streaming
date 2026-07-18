from pyspark.sql import DataFrame

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET
)


def configure_minio(spark):

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.endpoint",
        f"http://{MINIO_ENDPOINT}"
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        MINIO_ACCESS_KEY
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        MINIO_SECRET_KEY
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true"
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false"
    )

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )


def write_batch():

    def _write(batch_df: DataFrame, batch_id: int):

        print(f"Batch {batch_id} -> MinIO")

        (
            batch_df.write
            .mode("append")
            .parquet(
                f"s3a://{MINIO_BUCKET}/votes/"
            )
        )

    return _write
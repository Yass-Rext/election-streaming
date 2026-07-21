"""Configuration runtime Spark Structured Streaming."""

from __future__ import annotations

import os

# ===============================
# Kafka
# ===============================

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "votes")
KAFKA_STARTING_OFFSETS: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

# ===============================
# PostgreSQL
# ===============================

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "election")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")

POSTGRES_URL: str = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ===============================
# MinIO (S3A)
# ===============================

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "votes")

# ===============================
# Streaming
# ===============================

CHECKPOINT_BASE: str = os.getenv("CHECKPOINT_BASE", "checkpoints")
TRIGGER_INTERVAL: str = os.getenv("TRIGGER_INTERVAL", "5 seconds")
SHUFFLE_PARTITIONS: str = os.getenv("SHUFFLE_PARTITIONS", "4")

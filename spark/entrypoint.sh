#!/bin/bash
set -euo pipefail

echo "[spark-app] Attente des dépendances..."

wait_for() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-60}"

  echo "[spark-app] Attente $name ($host:$port)..."
  for i in $(seq 1 "$retries"); do
    if (echo > "/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      echo "[spark-app] $name disponible"
      return 0
    fi
    sleep 2
  done
  echo "[spark-app] ERREUR: $name indisponible après ${retries} tentatives" >&2
  exit 1
}

wait_for "${KAFKA_BOOTSTRAP_SERVERS%%:*}" 9092 "Kafka"
wait_for "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}" "PostgreSQL"
wait_for "${MINIO_ENDPOINT%%:*}" 9000 "MinIO"
wait_for "spark-master" 7077 "Spark Master"

# Petite marge pour l'init MinIO bucket (service mc)
sleep 5

echo "[spark-app] Lancement spark-submit..."

exec /opt/spark/bin/spark-submit \
  --master "spark://spark-master:7077" \
  --deploy-mode client \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST:-spark-app}" \
  --conf "spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS:-4}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /app/app.py

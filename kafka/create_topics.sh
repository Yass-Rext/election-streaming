#!/usr/bin/env bash
# Crée le topic Kafka applicatif (idempotent).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-votes}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

echo "Création du topic '${TOPIC}' sur ${BOOTSTRAP}..."

kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}"

kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
echo "Topic prêt."

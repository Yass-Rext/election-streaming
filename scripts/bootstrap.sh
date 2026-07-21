#!/usr/bin/env bash
# Bootstrap local : build + up + attente santé services.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -f .env ]]; then
  echo "Fichier .env absent — copie depuis .env.example"
  cp .env.example .env
fi

echo "==> Build & démarrage"
docker compose up --build -d

echo "==> Attente PostgreSQL"
./scripts/wait-for-it.sh localhost:5432 -t 60 -- echo "PostgreSQL OK"

echo "==> Attente Kafka"
./scripts/wait-for-it.sh localhost:9092 -t 90 -- echo "Kafka OK"

echo "==> Attente Dashboard"
./scripts/wait-for-it.sh localhost:8501 -t 90 -- echo "Dashboard OK"

echo ""
echo "Stack démarrée."
echo "  Kafka UI      : http://localhost:8080"
echo "  Spark UI      : http://localhost:8081"
echo "  Streamlit     : http://localhost:8501"
echo "  MinIO Console : http://localhost:9001"
echo "  PgAdmin       : http://localhost:5050"
echo ""
echo "Logs Spark : docker compose logs -f spark-app"

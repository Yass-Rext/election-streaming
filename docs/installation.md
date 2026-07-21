# Installation — from scratch

Ce guide décrit l’installation complète sur une machine de développement (Windows, macOS ou Linux) à partir d’un dépôt vide de dépendances locales.

## Prérequis

| Outil | Version recommandée | Rôle |
| ----- | ------------------- | ---- |
| Docker Desktop / Engine | ≥ 24 | Conteneurs |
| Docker Compose | v2 (`docker compose`) | Orchestration |
| Git | ≥ 2.30 | Clonage |
| RAM disponible | ≥ 8 Go | Spark + Kafka + PG |
| Ports libres | 5050, 5432, 7077, 8080, 8081, 8501, 9000, 9001, 9092 | Interfaces |

Vérification :

```bash
docker --version
docker compose version
git --version
```

## 1. Cloner le dépôt

```bash
git clone https://github.com/Yass-Rext/election-streaming.git
cd election-streaming
```

## 2. Configurer l’environnement

```bash
cp .env.example .env
```

Variables minimales (valeurs par défaut adaptées au Compose) :

```env
POSTGRES_DB=election
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

PGADMIN_DEFAULT_EMAIL=admin@election.sn
PGADMIN_DEFAULT_PASSWORD=admin

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_BUCKET=votes

KAFKA_TOPIC=votes
VOTE_INTERVAL_SECONDS=2
SENEGAL_VOTE_RATIO=0.80
TRIGGER_INTERVAL=5 seconds
SHUFFLE_PARTITIONS=4
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
```

> Ne committez jamais `.env`. Le fichier `.gitignore` l’exclut déjà ; utilisez `.env.example` comme modèle.

## 3. Lancer la stack

```bash
docker compose up --build
```

Ou en arrière-plan :

```bash
docker compose up --build -d
```

Alternative scriptée (Linux/macOS / Git Bash) :

```bash
./scripts/bootstrap.sh
```

Le script copie `.env` si besoin, démarre Compose et attend Kafka / PostgreSQL / Streamlit.

## 4. Vérifier que tout est sain

```bash
docker compose ps
```

Services attendus : `kafka`, `kafka-ui`, `producer`, `spark-master`, `spark-worker`, `spark-app`, `postgres`, `pgadmin`, `minio`, `dashboard`. Le service `mc` doit être en statut **exited (0)** après création du bucket.

Logs utiles :

```bash
docker compose logs -f producer
docker compose logs -f spark-app
docker compose logs -f dashboard
```

## 5. Ouvrir les interfaces

| Interface | URL | Identifiants |
| --------- | --- | ------------ |
| Kafka UI | http://localhost:8080 | — |
| Spark UI | http://localhost:8081 | — |
| Streamlit | http://localhost:8501 | — |
| MinIO | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| PgAdmin | http://localhost:5050 | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` |

## 6. Première validation fonctionnelle

1. Dans Kafka UI : topic `votes` avec messages en croissance.
2. Dans les logs `spark-app` : lignes `Début batch` / `PostgreSQL OK` / `MinIO OK`.
3. Dans Streamlit : métriques non nulles après ~10–30 s.
4. Dans MinIO : préfixe `votes/` avec fichiers `.parquet`.
5. Dans PgAdmin / `psql` : `SELECT COUNT(*) FROM votes_bruts;`

## 7. Arrêt et nettoyage

Arrêt sans suppression des volumes :

```bash
docker compose down
```

Arrêt **avec** suppression des données persistantes (PostgreSQL, MinIO, checkpoints Spark) :

```bash
docker compose down -v
```

## Installation sans Docker (hors scope nominal)

Le projet est conçu pour Compose. Un run « nu » nécessiterait d’installer manuellement Kafka, Spark 3.5.1, PostgreSQL 16, MinIO, puis de pointer les variables d’environnement vers `localhost`. Ce mode n’est pas documenté comme supporté ; préférez Docker.

## Problèmes au premier démarrage

Consultez [troubleshooting.md](troubleshooting.md). Causes fréquentes :

- ports déjà occupés ;
- `.env` manquant ou variables `POSTGRES_*` vides ;
- image Spark en cours de téléchargement des packages Maven (premier démarrage long) ;
- `spark-app` redémarre tant que Kafka / Postgres / MinIO / Master ne sont pas prêts.

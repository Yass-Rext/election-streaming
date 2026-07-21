# Documentation — Election Streaming

Documentation technique complète du projet **election-streaming** : pipeline de simulation et de traitement en temps réel des votes électoraux (Sénégal + diaspora).

## Architecture en une phrase

**Producer Python → Kafka KRaft (`votes`) → Spark Structured Streaming (un seul `foreachBatch`) → PostgreSQL + MinIO Parquet → Dashboard Streamlit (lecture PostgreSQL uniquement).**

## Guide de lecture

| Document | Contenu |
| -------- | ------- |
| [architecture.md](architecture.md) | Vue d’ensemble, composants, diagrammes Mermaid |
| [installation.md](installation.md) | Installation from scratch (prérequis, `.env`, premier lancement) |
| [docker.md](docker.md) | Services Compose, volumes, réseau, variables d’environnement |
| [kafka.md](kafka.md) | Topic `votes`, mode KRaft, commandes de test |
| [spark.md](spark.md) | SparkSession, packages, `foreachBatch`, checkpoints, transformations |
| [postgres.md](postgres.md) | Schéma complet : tables et colonnes |
| [minio.md](minio.md) | Bucket, Parquet, configuration S3A |
| [dashboard.md](dashboard.md) | Pages Streamlit et lectures SQL |
| [producer.md](producer.md) | Schéma des votes, ajout candidat / région |
| [streaming.md](streaming.md) | Flux de données de bout en bout |
| [troubleshooting.md](troubleshooting.md) | Incidents fréquents et correctifs |
| [deployment.md](deployment.md) | Déploiement from zero, rebuild des conteneurs |
| [optimisation.md](optimisation.md) | Persist, shuffle, partitions, JDBC, MinIO |
| [changelog.md](changelog.md) | Bugs identifiés, correctifs et évolutions |

## Rapport exécutif

Le rapport de synthèse du projet se trouve à la racine :

- [../RAPPORT_FINAL.md](../RAPPORT_FINAL.md)

## Démarrage rapide

```bash
cp .env.example .env
docker compose up --build
```

| Interface | URL |
| --------- | --- |
| Kafka UI | http://localhost:8080 |
| Spark UI | http://localhost:8081 |
| Streamlit | http://localhost:8501 |
| MinIO Console | http://localhost:9001 |
| PgAdmin | http://localhost:5050 |

## Structure applicative (chemins utiles)

```text
election-streaming/
├── producer/app/          # Génération et publication Kafka
├── spark/app/             # Job Structured Streaming (app.py, sinks, …)
├── postgres/init.sql      # Schéma PostgreSQL (init Docker)
├── dashboard/             # Streamlit (Home.py + pages/)
├── kafka/create_topics.sh
├── scripts/bootstrap.sh
└── docs/                  # Cette documentation
```

## Convention

Tous les documents de ce dossier sont rédigés en **français**. Les exemples de commandes ciblent un environnement **Docker Compose** local.

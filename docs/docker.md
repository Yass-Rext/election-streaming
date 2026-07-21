# Docker Compose — services, volumes, réseau, variables

Fichier de référence : `docker-compose.yml` (projet nommé `election-streaming`).

## Réseau

| Nom | Driver | Usage |
| --- | ------ | ----- |
| `election-net` | `bridge` | DNS interne entre tous les services |

Tous les services déclarent `networks: [election-net]`.

## Volumes nommés

| Volume | Monté sur | Contenu |
| ------ | --------- | ------- |
| `postgres_data` | `postgres:/var/lib/postgresql/data` | Données PostgreSQL persistantes |
| `minio_data` | `minio:/data` | Objets MinIO |
| `spark_checkpoints` | `spark-app:/app/checkpoints` | Checkpoints Structured Streaming |
| `spark_ivy` | master/worker `/tmp/.ivy2` et app `/root/.ivy2` | Cache des jars Maven (`--packages`) |

> Correctif historique : `postgres_data` était déclaré mais **non monté** — la base était perdue à chaque `down`. Le montage est désormais explicite.

## Catalogue des services

### `kafka`

| Attribut | Valeur |
| -------- | ------ |
| Image | `apache/kafka:3.7.1` |
| Ports | `9092:9092`, `9093:9093` |
| Mode | KRaft (`broker,controller`) |
| Healthcheck | `kafka-topics.sh --list` |

Variables clés : `KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092`, `CLUSTER_ID`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.

### `kafka-ui`

| Attribut | Valeur |
| -------- | ------ |
| Image | `provectuslabs/kafka-ui:v0.7.2` |
| Port | `8080:8080` |
| Depends on | `kafka` (healthy) |

Bootstrap UI : `kafka:9092`, cluster name `election-cluster`.

### `producer`

| Attribut | Valeur |
| -------- | ------ |
| Build | `./producer` |
| Restart | `on-failure` |
| Volume | `./producer/app:/app` (hot-reload code) |
| Depends on | `kafka` (healthy) |

Variables :

| Variable | Défaut | Rôle |
| -------- | ------ | ---- |
| `KAFKA_SERVER` | `kafka:9092` | Bootstrap |
| `KAFKA_TOPIC` | `${KAFKA_TOPIC:-votes}` | Topic cible |
| `VOTE_INTERVAL_SECONDS` | `2` | Pause entre votes |
| `SENEGAL_VOTE_RATIO` | `0.80` | Part des votes nationaux |

### `spark-master`

| Attribut | Valeur |
| -------- | ------ |
| Image | `apache/spark:3.5.1` |
| Ports | `8081:8080` (UI), `7077:7077` |
| Healthcheck | TCP `7077` |

### `spark-worker`

| Attribut | Valeur |
| -------- | ------ |
| Image | `apache/spark:3.5.1` |
| Mémoire / cœurs | `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES` |
| Depends on | `spark-master` (healthy) |

### `spark-app`

| Attribut | Valeur |
| -------- | ------ |
| Build | `./spark` (`COPY app/` + `entrypoint.sh`) |
| Volumes | `./spark/app:/app`, `spark_checkpoints`, `spark_ivy` |
| Depends on | kafka, postgres, minio, spark-master, mc (completed), spark-worker |

Variables runtime :

| Variable | Source | Rôle |
| -------- | ------ | ---- |
| `SPARK_DRIVER_HOST` | `spark-app` | Hostname driver (client mode) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Lecture stream |
| `KAFKA_TOPIC` | `.env` | Subscribe |
| `KAFKA_STARTING_OFFSETS` | `earliest` | Reprise |
| `POSTGRES_*` | `.env` | JDBC |
| `MINIO_ENDPOINT` | `minio:9000` | S3A |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | root MinIO | Auth S3A |
| `MINIO_BUCKET` | `votes` | Bucket |
| `TRIGGER_INTERVAL` | `5 seconds` | Micro-batch |
| `SHUFFLE_PARTITIONS` | `4` | Shuffle SQL |

### `postgres`

| Attribut | Valeur |
| -------- | ------ |
| Image | `postgres:16` |
| Port | `5432:5432` |
| Init | `./postgres/init.sql` → `/docker-entrypoint-initdb.d/init.sql:ro` |
| Volume | `postgres_data` |
| Healthcheck | `pg_isready` |

### `pgadmin`

| Attribut | Valeur |
| -------- | ------ |
| Image | `dpage/pgadmin4:8.14` |
| Port | `5050:80` |
| Auth | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` |

### `minio`

| Attribut | Valeur |
| -------- | ------ |
| Image | `minio/minio:RELEASE.2024-12-18T13-15-44Z` |
| Ports | `9000:9000`, `9001:9001` |
| Commande | `server /data --console-address ":9001"` |
| Volume | `minio_data` |

### `mc` (init bucket)

| Attribut | Valeur |
| -------- | ------ |
| Image | `minio/mc:RELEASE.2024-11-17T19-35-56Z` |
| Rôle | One-shot : `mc alias set` + `mc mb --ignore-existing` |
| Credentials | Issues de `MINIO_ROOT_*` (plus de secrets en dur) |

### `dashboard`

| Attribut | Valeur |
| -------- | ------ |
| Build | `./dashboard` |
| Port | `8501:8501` |
| Depends on | `postgres` (healthy) |
| Env | `POSTGRES_HOST=postgres` + credentials `.env` |

## Variables d’environnement (fichier `.env`)

Référence complète : `.env.example`.

| Groupe | Variables |
| ------ | --------- |
| PostgreSQL | `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| PgAdmin | `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` |
| MinIO | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET` |
| Kafka / Producer | `KAFKA_TOPIC`, `VOTE_INTERVAL_SECONDS`, `SENEGAL_VOTE_RATIO` |
| Spark | `TRIGGER_INTERVAL`, `SHUFFLE_PARTITIONS`, `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES` |

## Images épinglées

Les tags sont **versionnés** (Kafka 3.7.1, Spark 3.5.1, Postgres 16, Kafka UI v0.7.2, PgAdmin 8.14, MinIO / mc avec dates de release) pour des builds reproductibles.

## Commandes Compose usuelles

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f spark-app
docker compose restart producer
docker compose build --no-cache spark-app
docker compose down
docker compose down -v   # reset volumes
```

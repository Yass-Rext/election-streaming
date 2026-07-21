# MinIO — bucket, Parquet et configuration S3A

## Rôle

MinIO joue le rôle de **data lake** : archivage append-only des votes enrichis au format **Parquet Snappy**, indépendamment du dashboard (qui lit PostgreSQL).

## Service Docker

| Élément | Valeur |
| ------- | ------ |
| Image | `minio/minio:RELEASE.2024-12-18T13-15-44Z` |
| API | `http://minio:9000` (hôte : `localhost:9000`) |
| Console | `http://localhost:9001` |
| Volume | `minio_data` → `/data` |
| Credentials | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |

## Initialisation du bucket (`mc`)

Le service one-shot `mc` :

1. Attend MinIO (`mc alias set local http://minio:9000 …`)
2. Crée le bucket : `mc mb --ignore-existing local/${MINIO_BUCKET}`
3. Quitte avec code 0

`spark-app` dépend de `mc` avec `condition: service_completed_successfully`.

| Variable | Défaut |
| -------- | ------ |
| `MINIO_BUCKET` | `votes` |

> Correctif : les credentials n’étaient plus en dur dans `mc` ; elles viennent du `.env`.

## Chemin d’écriture Spark

```text
s3a://votes/votes/
```

Implémentation (`spark/app/sinks/minio.py`) :

```python
df.write.mode("append").option("compression", "snappy").parquet(target)
```

Chaque micro-batch ajoute des fichiers Parquet sous le préfixe `votes/`.

## Configuration Hadoop S3A

Appliquée dans `configure_minio(spark)` :

| Propriété | Valeur |
| --------- | ------ |
| `fs.s3a.endpoint` | `http://minio:9000` |
| `fs.s3a.access.key` | `MINIO_ACCESS_KEY` |
| `fs.s3a.secret.key` | `MINIO_SECRET_KEY` |
| `fs.s3a.path.style.access` | `true` |
| `fs.s3a.connection.ssl.enabled` | `false` |
| `fs.s3a.impl` | `org.apache.hadoop.fs.s3a.S3AFileSystem` |
| `fs.s3a.aws.credentials.provider` | `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` |
| `fs.s3a.fast.upload` | `true` |
| `fs.s3a.multipart.size` | `67108864` |

### Pourquoi `SimpleAWSCredentialsProvider` ?

Sans ce provider, Hadoop S3A tente des chaînes de credentials AWS (Instance Profile, etc.) incompatibles avec MinIO local → échecs d’auth. Ce réglage force l’usage des clés access/secret fournies.

### Pourquoi `aws-java-sdk-bundle` ?

Le package `hadoop-aws` dépend du SDK AWS. Son absence dans `--packages` provoquait des erreurs de classes manquantes au premier `write.parquet`.

## Vérifications

### Console web

1. Ouvrir http://localhost:9001  
2. Se connecter avec les identifiants `.env`  
3. Bucket `votes` → dossier `votes/` → objets `.parquet`

### CLI `mc` (conteneur temporaire)

```bash
docker run --rm --network election-streaming_election-net \
  -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD \
  minio/mc:RELEASE.2024-11-17T19-35-56Z \
  sh -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls local/votes/votes/'
```

### Logs Spark

```bash
docker compose logs spark-app | findstr /i "MinIO"
# Linux/macOS
docker compose logs spark-app | grep -i MinIO
```

Attendu : `MinIO OK | path=s3a://votes/votes/ ...`

## Schéma des fichiers Parquet

Les colonnes correspondent au DataFrame post-`transform` (schéma vote + `ingestion_time`, `est_diaspora`, `tranche_age`). Utile pour des jobs batch ultérieurs (Spark, DuckDB, Athena-like, etc.).

## Remise à zéro

```bash
docker compose down
docker volume rm election-streaming_minio_data
docker compose up -d minio mc
```

Le service `mc` recrée le bucket vide.

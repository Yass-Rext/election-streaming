# Changelog — bugs, correctifs et évolutions

Historique détaillé des problèmes rencontrés lors de la mise au point du projet **election-streaming** et des correctifs appliqués. Les entrées sont regroupées par thème.

---

## Résumé

| # | Bug | Impact | Statut |
| - | --- | ------ | ------ |
| 1 | Mauvais emplacement code Spark vs Dockerfile/Compose | `spark-app` incapable de démarrer | Corrigé |
| 2 | Producer sans `vote_id` / `timestamp` / `cni` | Spark filtre 100 % des votes | Corrigé |
| 3 | Typo `prefession` | `profession` toujours null | Corrigé |
| 4 | Diaspora : `zone` au lieu de `continent` | Agrégations diaspora cassées | Corrigé |
| 5 | Volume `postgres_data` non monté | Aucune persistance PG | Corrigé |
| 6 | Dashboard fragile / pages vides | Crash UI, Monitoring/Pays absents | Corrigé |
| 7 | Multi-queries streaming / layout obsolète | Complexité, checkpoints multiples | Corrigé |
| 8 | MinIO : credentials provider + `mc` hardcodé | Échecs S3A / init bucket | Corrigé |
| 9 | Pas d’AWS SDK dans `--packages` | Écriture Parquet impossible | Corrigé |
| 10 | Stubs vides (scripts, topics, …) | Bootstrap incomplet | Corrigé |
| 11 | Producer ignore `KAFKA_TOPIC` | Topic non configurable | Corrigé |
| 12 | Logging / gestion d’erreurs faibles | Diagnostic difficile | Corrigé |

---

## 1. Structure Spark incohérente (`spark/*.py` vs `spark/app/`)

### Symptôme

Le conteneur `election-spark-app` plantait immédiatement : fichier `/app/app.py` introuvable, modules absents, ou image construite sans le code exécutable attendu.

### Cause racine

- Le **Dockerfile** exécutait `COPY app/ /app/`.
- **Compose** montait `./spark/app:/app`.
- Le code source se trouvait pourtant à la racine `spark/*.py` (hors `app/`).

Résultat : dossier monté / copié **vide ou incomplet** → job impossible à lancer.

### Correctif

- Restructuration complète sous `spark/app/` :
  - `app.py`, `config.py`, `constants.py`, `schemas.py`
  - `transformations.py`, `aggregations.py`
  - `sinks/postgres.py`, `sinks/minio.py`
- Alignement Dockerfile + volume Compose + `entrypoint.sh` (`spark-submit /app/app.py`).

### Fichiers concernés

`spark/Dockerfile`, `spark/entrypoint.sh`, `spark/app/**`, `docker-compose.yml` (service `spark-app`).

---

## 2. Schéma producer incomplet — filtrage total dans `clean_votes`

### Symptôme

Kafka affichait des messages, mais PostgreSQL restait vide, MinIO sans Parquet utiles, dashboard à zéro. Les logs Spark montraient des batches vides après transformation.

### Cause racine

Le filtre Spark exige notamment :

```text
vote_id IS NOT NULL AND vote_id != ''
```

Le producer n’émettait pas `vote_id`, ni un `timestamp` / `cni` conformes au schéma Spark. Après `from_json`, `vote_id` était null → **tous** les votes écartés.

### Correctif

Alignement du schéma producer (`generator.py`) sur `spark/app/schemas.py` :

- `vote_id` = UUID
- `timestamp` = ISO 8601 UTC
- `cni` = identifiant fictif
- champs géographiques et `candidat` cohérents

### Leçon

Tout changement de schéma doit être validé **bout-en-bout** (un message Kafka → une ligne `votes_bruts`).

---

## 3. Typo `prefession` vs `profession`

### Symptôme

Colonne `profession` toujours null ; table `resultats_profession` vide malgré des votes valides.

### Cause

Champ JSON mal orthographié côté génération (`prefession`), alors que Spark et PostgreSQL attendaient `profession`.

### Correctif

Renommage systématique en `profession` dans le generator et les données de référence (`professions.json`).

---

## 4. Diaspora : `zone` au lieu de `continent`

### Symptôme

`resultats_diaspora` incorrecte ou vide ; pages Diaspora / Pays sans données pertinentes.

### Cause

Le référentiel et/ou le payload utilisaient une clé `zone`. L’agrégation Spark fait :

```text
groupBy("continent", "pays")
```

Incompatibilité de nom de colonne.

### Correctif

- `zone_diaspora.json` structuré par **continent**
- generator : clé `continent`
- documentation et agrégations alignées

---

## 5. Volume PostgreSQL déclaré mais non monté

### Symptôme

Après `docker compose down` puis `up`, la base était réinitialisée (ou données perdues). Impression d’instabilité du pipeline.

### Cause

```yaml
volumes:
  postgres_data:   # déclaré
```

mais le service `postgres` **ne montait pas** `postgres_data:/var/lib/postgresql/data`.

### Correctif

Montage explicite du volume nommé + conservation de `init.sql` pour le premier boot.

---

## 6. Dashboard : duplication, crashes, pages vides

### Symptômes

- Erreurs Streamlit sur DataFrames vides (`.sum()`, Plotly).
- `load_table` dupliqué / incohérent.
- `Monitoring.py` et `Pays.py` vides (stubs).
- Pages départements fragiles.

### Correctifs

- `database.py` unique : `load_table` tolérant aux erreurs, cache TTL 5 s.
- Helpers `table_is_empty` / `show_empty_state`.
- Implémentation complète de **Departements**, **Pays**, **Monitoring**.
- Garde empty-state sur toutes les pages de visualisation.

---

## 7. Multiples requêtes streaming et documentation obsolète

### Symptômes

Plusieurs `writeStream` (un par sink / agrégation) → checkpoints multiples, ordering difficile, recommandations d’archi dépassées dans le README.

### Correctif

- **Une seule** query : `election_main_pipeline` + `foreachBatch(process_batch)`.
- Dans le batch : MinIO → `votes_bruts` → recalcul agrégats.
- README / docs mis à jour (chemins `spark/app/`, table `votes_bruts`, etc.).

---

## 8. MinIO : provider de credentials et `mc` hardcodé

### Symptômes

Échecs d’authentification S3A ; bucket parfois créé avec de mauvais comptes ; divergence avec `.env`.

### Correctifs

- `fs.s3a.aws.credentials.provider` = `SimpleAWSCredentialsProvider`
- Service `mc` lit `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` / `MINIO_BUCKET`
- Path-style access + SSL désactivé pour le lab local

---

## 9. Absence du AWS SDK dans `spark-submit --packages`

### Symptôme

`ClassNotFoundException` / erreurs Hadoop S3A lors du premier `write.parquet`.

### Cause

`hadoop-aws` seul ne suffit pas : il faut le bundle SDK AWS compatible.

### Correctif

Ajout de `com.amazonaws:aws-java-sdk-bundle:1.12.262` dans `entrypoint.sh` et `SPARK_PACKAGES` (`app.py`).

---

## 10. Stubs vides (scripts, Kafka topics, etc.)

### Symptôme

Scripts présents dans l’arborescence mais vides ou non fonctionnels ; création de topic non documentée / non automatisable.

### Correctif

- `kafka/create_topics.sh` : création idempotente du topic
- `scripts/bootstrap.sh` : up + wait
- `scripts/wait-for-it.sh` : attente TCP
- `.env.example`, `.gitignore`
- `postgres/schema.sql` miroir documentaire
- documentation `docs/` complète

---

## 11. Producer ignorait `KAFKA_TOPIC`

### Symptôme

Changer `KAFKA_TOPIC` dans `.env` / Compose n’avait aucun effet ; topic hardcodé « votes ».

### Correctif

`config.KAFKA_TOPIC = os.getenv(...)` utilisé par `main.py` → `send_vote(KAFKA_TOPIC, vote)`.

---

## 12. Logging et gestion d’erreurs insuffisants

### Symptômes

Échecs silencieux, stacktraces absentes, difficile de savoir quel batch / quelle table échouait.

### Correctifs

- Logging structuré (`asctime | level | name | message`) sur producer et spark-app
- `logger.exception` dans les sinks et `process_batch`
- Retries Kafka au démarrage producer
- Arrêt gracieux (signaux / `finally` SparkSession)
- Dashboard : exceptions SQL avalées → DataFrame vide + log, UI stable

---

## Améliorations associées (non-bugs)

- Table **`votes_bruts`** + indexes
- Healthchecks Compose (Kafka, Postgres, Spark Master sur UI `:8080`)
- Images MinIO en `latest` (tags RELEASE invalides retirés)
- Recalcul des agrégations en **SQL PostgreSQL natif** (`refresh_aggregations_sql`) — bien plus rapide qu’un full scan Spark JDBC
- Persist `MEMORY_AND_DISK` sur le micro-batch
- Volume `spark_checkpoints` + `spark_ivy`
- Trigger par défaut : `10 seconds`

---

## Correctifs runtime post-démarrage

| Problème | Correctif |
| -------- | --------- |
| Tag `minio/mc:RELEASE.2024-11-17...` introuvable | `minio/mc:latest` + `minio/minio:latest` |
| Healthcheck Spark Master sur `:7077` (bind hostname only) | Check TCP sur UI `:8080` |
| Batches > trigger (relecture Spark de `votes_bruts`) | Agrégations SQL côté Postgres |

---

## Versions documentaires

| Date | Note |
| ---- | ---- |
| 2026-07-19 | Audit complet, corrections, docs, vérification E2E Docker réussie |
| 2026-07 | Première documentation française complète (`docs/`, `RAPPORT_FINAL.md`, README aligné) |

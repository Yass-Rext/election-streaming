# Rapport final — Election Streaming

**Projet** : election-streaming  
**Nature** : plateforme pédagogique de Data Engineering temps réel  
**Périmètre** : simulation de votes (Sénégal + diaspora), streaming, stockage, visualisation  
**Stack** : Python, Kafka KRaft, Spark Structured Streaming 3.5.1, PostgreSQL 16, MinIO, Streamlit, Docker Compose  

---

## 1. Synthèse exécutive

Le projet délivre un pipeline bout-en-bout opérationnel :

**Producer Python → Kafka (topic `votes`) → Spark Structured Streaming (un `foreachBatch`) → PostgreSQL (`votes_bruts` + agrégations) + MinIO (Parquet) → Dashboard Streamlit (lecture PostgreSQL uniquement).**

La phase de stabilisation a révélé **douze classes de bugs critiques** (structure Docker/Spark, schéma des messages, persistance, MinIO/S3A, dashboard). Tous ont été corrigés. La documentation française (`docs/`) et ce rapport formalisent l’architecture réelle, les procédures de déploiement et les commandes de test.

Lancement de référence :

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

---

## 2. Bugs identifiés (détail)

### 2.1 Code Spark inaccessible au runtime

Le code était sous `spark/*.py` alors que le Dockerfile faisait `COPY app/` et Compose montait `./spark/app`. Le service `spark-app` ne pouvait pas exécuter le job.

### 2.2 Filtrage total des votes

Le producer n’envoyait pas `vote_id`, `timestamp` et `cni` conformes. `clean_votes` exige un `vote_id` non null → **aucun** vote ne traversait le pipeline.

### 2.3 Typo `prefession`

La profession n’était jamais peuplée ; `resultats_profession` restait vide.

### 2.4 Champ diaspora `zone` vs `continent`

Les agrégations `groupBy("continent", "pays")` ne correspondaient pas au payload → KPI diaspora incorrects.

### 2.5 Volume PostgreSQL non monté

`postgres_data` déclaré sans bind sur `/var/lib/postgresql/data` → perte de persistance.

### 2.6 Dashboard instable

`load_table` dupliqué ; crashes sur tables vides ; `Monitoring.py` et `Pays.py` vides ; pages départements fragiles.

### 2.7 Architecture streaming dispersée

Plusieurs requêtes `writeStream`, checkpoints multiples, README / recommandations obsolètes.

### 2.8 MinIO mal authentifié

Absence de `SimpleAWSCredentialsProvider` ; credentials `mc` en dur, non alignés sur `.env`.

### 2.9 Packages Spark incomplets

Manque de `aws-java-sdk-bundle` dans `--packages` → échec écriture S3A/Parquet.

### 2.10 Artefacts stubs

Scripts (`bootstrap`, `wait-for-it`, `create_topics`) et fichiers d’exemple incomplets ou vides.

### 2.11 `KAFKA_TOPIC` ignoré

Le producer ne respectait pas la variable d’environnement Compose / `.env`.

### 2.12 Observabilité faible

Peu de logs structurés, erreurs mal remontées, diagnostic long.

Le détail narratif figure dans [docs/changelog.md](docs/changelog.md).

---

## 3. Correctifs appliqués

| Domaine | Correctif |
| ------- | --------- |
| Spark | Restructuration `spark/app/`, pipeline unique `foreachBatch`, transforms/aggregations/sinks modulaires |
| Producer | Schéma aligné (`vote_id`, `timestamp`, `cni`, `profession`, `continent`), respect de `KAFKA_TOPIC` |
| Docker | Healthchecks, images épinglées, volumes PG/MinIO/checkpoints/ivy, `mc` paramétré par env |
| PostgreSQL | Table `votes_bruts` + indexes ; agrégations en overwrite+truncate ; `dropDuplicates(vote_id)` |
| MinIO | S3A + SimpleAWSCredentialsProvider + SDK AWS ; Parquet Snappy append |
| Dashboard | `database.py` robuste ; empty-states ; pages Departements, Pays, Monitoring complètes |
| Qualité | Logging professionnel, retries Kafka, arrêts gracieux |
| Projet | `.env.example`, `.gitignore`, scripts, documentation `docs/` |

---

## 4. Fichiers créés ou profondément modifiés

### Créés / structurés

| Zone | Éléments |
| ---- | -------- |
| `spark/app/` | `app.py`, `config.py`, `constants.py`, `schemas.py`, `transformations.py`, `aggregations.py`, `sinks/*` |
| `spark/` | `Dockerfile`, `entrypoint.sh`, `requirements.txt` |
| `producer/app/` | modules + `data/*.json` |
| `dashboard/` | `Home.py`, `database.py`, `pages/*` |
| `postgres/` | `init.sql`, `schema.sql` |
| `kafka/` | `create_topics.sh` |
| `scripts/` | `bootstrap.sh`, `wait-for-it.sh` |
| Racine | `docker-compose.yml`, `.env.example`, `.gitignore` |
| Docs | `docs/*.md`, `RAPPORT_FINAL.md`, `README.md` |

### Modifiés de façon critique

- Alignement Compose ↔ Dockerfiles ↔ chemins applicatifs
- Schéma vote bout-en-bout (producer ↔ Spark ↔ PG)
- Montage `postgres_data`
- Packages et conf S3A Spark

---

## 5. Améliorations par axe

### 5.1 Architecture

- Séparation claire producer / bus / processing / serving / UI
- Une seule requête de streaming (simplicité opérationnelle)
- Double stockage : OLTP agrégé (PostgreSQL) + lake Parquet (MinIO)
- Dashboard découplé (ne lit que PG)

### 5.2 Performance

- `persist` / `unpersist` sur batches et historique
- `SHUFFLE_PARTITIONS` configurable, AQE, Kryo
- JDBC `batchsize` + truncate overwrite
- Upload S3A rapide / multipart
- Cache Ivy pour jars Maven
- Cache Streamlit TTL 5 s

### 5.3 Sécurité (niveau lab)

- Secrets externalisés dans `.env` (non versionné)
- Plus de credentials MinIO en dur dans `mc`
- Validation basique des noms de tables dashboard
- **Limite assumée** : pas de TLS, pas d’auth Kafka/Spark, mots de passe de démo — inadapté à une exposition Internet sans durcissement

### 5.4 Qualité

- Healthchecks et `depends_on` conditionnels
- Logs structurés et exceptions tracées
- Empty-states UI
- Scripts de bootstrap et de création de topic
- Documentation française professionnelle avec diagrammes Mermaid

---

## 6. Tables PostgreSQL livrées

`votes_bruts`, `resultats_candidats`, `resultats_regions`, `resultats_departements`, `resultats_bureaux`, `resultats_diaspora`, `participation_sexe`, `participation_age`, `resultats_profession`.

---

## 7. Extension métier

| Besoin | Fichier |
| ------ | ------- |
| Ajouter un candidat | `producer/app/data/candidats.json` |
| Ajouter région / département / centres | `producer/app/data/centres_vote.json` |
| Ajouter zone diaspora | `producer/app/data/zone_diaspora.json` |

Puis `docker compose restart producer`.

---

## 8. Plan de tests (commandes)

### 8.1 Kafka

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic votes \
  --from-beginning --max-messages 5
```

Critère : JSON avec `vote_id`, `timestamp`, `type`, `candidat`, `profession`, et `continent` si diaspora.

### 8.2 Spark

```bash
docker compose logs -f spark-app
```

Critères : `SparkSession démarrée`, `Début batch`, `MinIO OK`, `PostgreSQL OK`, `Fin batch`.  
UI : http://localhost:8081 — application `ElectionStreaming` active.

### 8.3 PostgreSQL

```bash
docker exec -it postgres-election psql -U postgres -d election -c "\dt"
docker exec -it postgres-election psql -U postgres -d election \
  -c "SELECT COUNT(*) AS n FROM votes_bruts;"
docker exec -it postgres-election psql -U postgres -d election \
  -c "SELECT * FROM resultats_candidats ORDER BY nb_votes DESC;"
```

Critères : 9 tables présentes ; `n` croissant ; résultats candidats non vides après quelques batches.

### 8.4 MinIO

1. Console http://localhost:9001 (identifiants `.env`)
2. Bucket `votes` → préfixe `votes/` → fichiers `.parquet`
3. Logs : `MinIO OK | path=s3a://votes/votes/`

### 8.5 Dashboard / bout-en-bout

1. Ouvrir http://localhost:8501
2. Vérifier les KPI Home > 0
3. Parcourir Résultats, Régions, Départements, Diaspora, Pays, Participation, Monitoring
4. Monitoring : tables en statut OK, connexion PG verte

Scénario E2E minimal :

```text
docker compose up --build -d
→ attendre ~60 s
→ message Kafka présent
→ ligne dans votes_bruts
→ objet Parquet MinIO
→ graphique Streamlit alimenté
```

---

## 9. Documentation associée

Index : [docs/README.md](docs/README.md)

| Document | Sujet |
| -------- | ----- |
| [docs/architecture.md](docs/architecture.md) | Architecture et Mermaid |
| [docs/installation.md](docs/installation.md) | Install from scratch |
| [docs/docker.md](docs/docker.md) | Compose exhaustif |
| [docs/kafka.md](docs/kafka.md) | Topic / KRaft / tests |
| [docs/spark.md](docs/spark.md) | Job streaming |
| [docs/postgres.md](docs/postgres.md) | Schéma SQL |
| [docs/minio.md](docs/minio.md) | Lake Parquet |
| [docs/dashboard.md](docs/dashboard.md) | Pages Streamlit |
| [docs/producer.md](docs/producer.md) | Schéma votes / extensions |
| [docs/streaming.md](docs/streaming.md) | Flux E2E |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Incidents |
| [docs/deployment.md](docs/deployment.md) | Déploiement / rebuild |
| [docs/optimisation.md](docs/optimisation.md) | Tuning |
| [docs/changelog.md](docs/changelog.md) | Historique bugs/fixes |

---

## 10. Conclusion

Le pipeline election-streaming est **fonctionnel, documenté et reproductible** via Docker Compose. Les défauts bloquants (code Spark hors chemin, schéma producer incompatible, persistance PG, MinIO/S3A, dashboard) ont été traités de façon structurelle plutôt que cosmétique. Les axes d’évolution naturels restent le durcissement sécurité, l’agrégation incrémentale à grande échelle, et l’orchestration type Kubernetes / supervision Prometheus — hors périmètre de la démo actuelle.

**Auteurs du projet** : Mamadou Yassarou Diallo, Fallou Diouk — cadre pédagogique Data Engineering.

---

## 11. Vérification runtime (2026-07-19)

Exécutée localement après `docker compose up --build` :

| Contrôle | Résultat |
| -------- | -------- |
| Producer | Votes envoyés en continu (`vote_id` UUID, schéma aligné) |
| Kafka | Healthy ; topic `votes` actif |
| Spark Master | Healthy (healthcheck UI `:8080`) |
| Spark App | Batches ~5 s après warm-up ; MinIO + PG OK |
| PostgreSQL | `votes_bruts` peuplé ; agrégations = total bruts |
| MinIO | Objets Parquet snappy sous `s3a://votes/votes/` |
| Dashboard | HTTP 200 sur `:8501` ; pages anti-crash tables vides |

**Optimisation post-audit** : recalcul des agrégations en SQL PostgreSQL natif (`psycopg2`) au lieu de relire toute `votes_bruts` via Spark JDBC à chaque batch — réduit fortement la durée des micro-batches.


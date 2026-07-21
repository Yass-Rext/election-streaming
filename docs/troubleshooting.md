# Dépannage — incidents fréquents et correctifs

## Méthode rapide

```bash
docker compose ps
docker compose logs --tail=100 spark-app
docker compose logs --tail=50 producer
docker compose logs --tail=50 kafka
```

Vérifier ensuite Kafka UI, Spark UI, Streamlit et MinIO.

---

## 1. `spark-app` redémarre en boucle / ModuleNotFound / fichier manquant

**Symptômes** : conteneur `Restarting`, logs « can't open file `/app/app.py` », structure vide.

**Causes historiques** :
- Code placé dans `spark/*.py` alors que Dockerfile fait `COPY app/` et Compose monte `./spark/app`.

**Correctif actuel** : code sous `spark/app/`. Vérifier :

```bash
dir spark\app   # Windows
ls spark/app    # Linux/macOS
docker compose build --no-cache spark-app
docker compose up -d spark-app
```

---

## 2. Dashboard à zéro alors que Kafka reçoit des messages

**Symptômes** : Kafka UI montre des messages ; `votes_bruts` vide ; logs Spark sans erreur évidente ou batches « 0 rows ».

**Cause classique** : schéma producer incomplet (`vote_id` / `timestamp` / `cni` absents) → `clean_votes` filtre tout.

**Vérification** :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic votes --from-beginning --max-messages 1
```

Le JSON doit contenir `"vote_id"`, `"timestamp"`, `"cni"`, `"profession"`, et pour la diaspora `"continent"` (pas `"zone"`).

---

## 3. Profession toujours absente / `resultats_profession` vide

**Cause** : typo `prefession` côté producer.

**Correctif** : champ `profession` dans `generator.py` et JSON.

---

## 4. Agrégations diaspora incorrectes

**Cause** : groupement sur `zone` au lieu de `continent`.

**Correctif** : producer émet `continent` ; Spark `votes_diaspora` groupBy `continent`, `pays`.

---

## 5. Données PostgreSQL perdues après `docker compose down`

**Cause** : volume `postgres_data` déclaré mais non monté.

**Correctif** : montage `postgres_data:/var/lib/postgresql/data` dans Compose.

Pour reset volontaire : `docker compose down -v`.

---

## 6. Échec écriture MinIO (credentials / ClassNotFound)

**Symptômes** : stacktrace S3A, `AmazonClientException`, classes AWS manquantes.

**Correctifs** :
- `SimpleAWSCredentialsProvider` dans `configure_minio`
- package `com.amazonaws:aws-java-sdk-bundle:1.12.262` dans `--packages`
- bucket créé par `mc` avec credentials `.env`

```bash
docker compose logs mc
docker compose restart spark-app
```

---

## 7. Premier démarrage Spark très long

**Cause** : téléchargement Maven des packages (Kafka, JDBC, Hadoop-AWS, SDK).

**Action** : patienter ; le volume `spark_ivy` accélère les redémarrages suivants.

---

## 8. Port déjà utilisé

```text
Bind for 0.0.0.0:8080 failed: port is already allocated
```

Libérer le port ou modifier le mapping dans `docker-compose.yml`. Ports critiques : 8080, 8081, 8501, 9001, 5050, 5432, 9092.

---

## 9. Kafka « NoBrokersAvailable » côté producer

**Cause** : producer démarré avant Kafka healthy, ou mauvais hostname (`localhost` dans Docker).

**Correctif** : `depends_on: condition: service_healthy` + retries dans `kafka_client.py`. Bootstrap interne = `kafka:9092`.

---

## 10. Pages Streamlit en erreur / crash

**Causes historiques** : `load_table` dupliqué, pages sans garde empty-state, `Monitoring.py` / `Pays.py` vides.

**Correctif** : `database.py` centralisé ; toutes les pages gèrent le vide.

```bash
docker compose restart dashboard
```

---

## 11. Topic vide / mauvais topic

Si `KAFKA_TOPIC` a été modifié dans `.env`, redémarrer **producer et spark-app** :

```bash
docker compose up -d producer spark-app
```

---

## 12. Checkpoints corrompus / offsets incohérents

```bash
docker compose stop spark-app
docker volume rm election-streaming_spark_checkpoints
docker compose up -d spark-app
```

Avec `startingOffsets=earliest`, Spark peut **rejouer** l’historique Kafka → doublons possibles dans `votes_bruts` (filtrés pour les agrégats via `dropDuplicates`).

---

## 13. `mc` en échec → `spark-app` ne démarre pas

```bash
docker compose logs mc
docker compose up mc
```

Vérifier `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` / `MINIO_BUCKET`.

---

## Commandes de diagnostic croisé

```bash
# Kafka
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Postgres
docker exec -it postgres-election psql -U postgres -d election -c "\dt"
docker exec -it postgres-election psql -U postgres -d election -c "SELECT COUNT(*) FROM votes_bruts;"

# Conteneurs
docker compose ps -a
```

Pour le détail des bugs et correctifs : [changelog.md](changelog.md).

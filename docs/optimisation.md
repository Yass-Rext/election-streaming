# Optimisation — Spark, JDBC et MinIO

Guide des leviers de performance déjà en place et des réglages recommandés pour un environnement local.

## 1. Persist / unpersist

Dans `process_batch` et `refresh_aggregations` :

```text
batch_df.persist(MEMORY_AND_DISK)
all_votes.persist(MEMORY_AND_DISK)
…
finally: unpersist()
```

**Pourquoi** : le même DataFrame est lu plusieurs fois (count, MinIO, append, puis N agrégations). Sans persist, Spark rejoue le plan (et potentiellement Kafka/transform) à chaque action.

**Attention** : trop de persist sur de gros batches peut saturer la mémoire worker (`SPARK_WORKER_MEMORY`).

## 2. Shuffle partitions

```env
SHUFFLE_PARTITIONS=4
```

Appliqué via `spark.sql.shuffle.partitions` (session + `spark-submit --conf`).

| Charge | Conseil |
| ------ | ------- |
| Démo locale (faible débit) | `2`–`4` |
| Plus de workers / plus de messages | `8`–`16` |

Trop de partitions → surcharge de tâches minuscules ; trop peu → skew et sous-utilisation CPU.

## 3. Adaptive Query Execution (AQE)

Activé : `spark.sql.adaptive.enabled=true`. Aide à coalescer les partitions de shuffle après agrégations.

## 4. Trigger et débit Kafka

| Paramètre | Défaut | Effet |
| --------- | ------ | ----- |
| `TRIGGER_INTERVAL` | `5 seconds` | Fréquence des micro-batches |
| `maxOffsetsPerTrigger` | `5000` | Cap de messages / batch |
| `VOTE_INTERVAL_SECONDS` | `2` | Débit source |

Pour stress-test : baisser `VOTE_INTERVAL_SECONDS` et augmenter `maxOffsetsPerTrigger` avec prudence.

Réduire `TRIGGER_INTERVAL` augmente la fraîcheur UI mais multiplie les recalculs d’agrégation (lecture complète de `votes_bruts`).

## 5. Stratégie d’agrégation

Actuellement : **recalcul total** depuis `votes_bruts` à chaque batch.

| Avantage | Inconvénient |
| -------- | ------------ |
| Simplicité, cohérence | Coût O(N) croissant |

Pistes d’évolution (non implémentées) :

- agrégations incrémentales / stateful `groupBy` streaming ;
- tables staging + merge ;
- materialiser seulement le delta du batch pour certaines KPI.

## 6. Déduplication

`dropDuplicates(["vote_id"])` avant agrégats :

- protège contre les rejeux at-least-once ;
- coût : shuffle sur l’historique.

Index PG `idx_votes_bruts_vote_id` accélère les filtres SQL externes, pas le shuffle Spark.

## 7. JDBC PostgreSQL

Optimisations dans `write_postgres` :

| Option | Valeur | Intérêt |
| ------ | ------ | ------- |
| `batchsize` | `1000` | Moins d’allers-retours |
| `truncate=true` (overwrite) | oui | Évite DROP TABLE, conserve indexes |
| `isolationLevel` | `READ_COMMITTED` | Cohérence raisonnable |

Éviter un `count()` systématique avant chaque write (déjà le cas pour les sinks — le count batch sert surtout au logging).

Lecture agrégats : full table scan de `votes_bruts` — acceptable en démo ; en prod, partitionner / indexer davantage / extraire via predicate pushdown.

## 8. MinIO / S3A

| Réglage | Effet |
| ------- | ----- |
| Compression `snappy` | Bon compromis CPU / taille |
| `fs.s3a.fast.upload=true` | Upload bufferisé |
| `multipart.size=64MB` | Gros objets découpés |
| `path.style.access=true` | Compatible MinIO |
| Volume `spark_ivy` | Évite re-download jars |

Écrire en **append** Parquet évite de réécrire l’historique à chaque batch (contrairement aux agrégats PG).

## 9. KryoSerializer

Réduit le coût de sérialisation vs Java serializer par défaut — utile dès que les shuffles augmentent.

## 10. Logging

Niveau applicatif `INFO`, SparkContext `WARN` : limite le bruit tout en gardant la durée des batches (`durée=%.2fs`).

Pour debugger un batch : temporairement monter le log level, puis revenir à WARN (I/O logs coûteux).

## 11. Dashboard

- Cache TTL 5 s : réduit la charge PG.
- Pool SQLAlchemy borné.
- Pas de jointures lourdes côté UI : tables déjà agrégées.

## 12. Checklist tuning local

1. Observer la durée des batches dans les logs `spark-app`.
2. Si durée >> trigger → augmenter mémoire worker ou réduire débit / fréquence.
3. Si CPU idle → augmenter `SHUFFLE_PARTITIONS` ou cœurs worker.
4. Si MinIO lent → vérifier disque Docker / antivirus hôte.
5. Si PG grossit trop → archiver `votes_bruts` vers MinIO-only et purger (évolution future).

## Ce qu’il ne faut pas faire en démo

- Multiplier les `writeStream` (une query par table) — déjà abandonné.
- `df.count()` avant chaque write de debug en production.
- `overwrite` sans `truncate` (recrée la table, perd indexes).
- Désactiver les checkpoints pour « aller plus vite » (risque de doublons non maîtrisés).

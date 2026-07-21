# Kafka — topic `votes`, KRaft et tests

## Mode KRaft

Le broker tourne **sans ZooKeeper**. Le processus combine les rôles `broker` et `controller` :

| Paramètre | Valeur |
| --------- | ------ |
| Image | `apache/kafka:3.7.1` |
| `KAFKA_PROCESS_ROLES` | `broker,controller` |
| `KAFKA_NODE_ID` | `1` |
| Quorum | `1@kafka:9093` |
| `CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk==` |
| Advertised | `PLAINTEXT://kafka:9092` |

Depuis les autres conteneurs, le bootstrap est toujours **`kafka:9092`**.

## Topic applicatif

| Propriété | Valeur |
| --------- | ------ |
| Nom | `votes` (surchargeable via `KAFKA_TOPIC`) |
| Création | Auto-création activée (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`) |
| Création manuelle | Script `kafka/create_topics.sh` |

Le script idempotent :

```bash
# Depuis l'hôte, en exécutant dans le conteneur kafka :
docker exec -it kafka bash /path/ou/copier/create_topics.sh

# Ou en ligne de commande équivalente :
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic votes \
  --partitions 3 \
  --replication-factor 1
```

Paramètres du script :

| Variable | Défaut |
| -------- | ------ |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` |
| `KAFKA_TOPIC` | `votes` |
| `KAFKA_PARTITIONS` | `3` |
| `KAFKA_REPLICATION_FACTOR` | `1` |

## Format des messages

- **Clé** : `vote_id` (UUID string) — utilisée par le producer pour le partitionnement.
- **Valeur** : JSON UTF-8, schéma décrit dans [producer.md](producer.md).

Exemple (extrait) :

```json
{
  "vote_id": "a1b2c3d4-...",
  "timestamp": "2026-07-19T01:23:45.678Z",
  "type": "SENEGAL",
  "cni": "1234567890",
  "profession": "Enseignant",
  "region": "Dakar",
  "candidat": "C001",
  "bureau": "BV-DAK-1"
}
```

## Kafka UI

- URL : http://localhost:8080  
- Cluster configuré : `election-cluster` → `kafka:9092`  
- Permet de visualiser le topic, les messages, le lag consommateur Spark.

## Commandes de test

### Lister les topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Décrire `votes`

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic votes
```

### Consommer les derniers messages

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic votes \
  --from-beginning \
  --max-messages 5
```

### Produire un message de test (manuel)

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic votes
```

Coller une ligne JSON valide (alignée sur le schéma Spark), puis `Entrée`.

### Vérifier que le producer publie

```bash
docker compose logs -f producer | findstr /i "Vote envoyé"
# Linux/macOS :
docker compose logs -f producer | grep -i "Vote envoyé"
```

### Groupes de consommateurs

Spark Structured Streaming crée un groupe de type Kafka source. Inspection :

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list
```

## Santé Compose

Le healthcheck Kafka exécute `--list` sur `localhost:9092`. Les services `producer`, `kafka-ui` et `spark-app` attendent `service_healthy` avant de démarrer pleinement.

## Points d’attention

1. **Advertised listeners** : les clients *dans* Docker doivent utiliser `kafka:9092`, pas `localhost:9092`.
2. **Mono-nœud** : `replication-factor=1` — acceptable en local uniquement.
3. **Topic renommé** : si vous changez `KAFKA_TOPIC`, alignez producer **et** spark-app, puis redémarrez les deux.
4. **Historique** : le producer ignorait autrefois `KAFKA_TOPIC` — corrigé dans `producer/app/config.py` + `main.py`.

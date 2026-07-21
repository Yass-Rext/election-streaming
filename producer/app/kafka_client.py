"""Client Kafka robuste pour l'envoi des votes."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config import (
    KAFKA_CONNECT_RETRIES,
    KAFKA_CONNECT_RETRY_DELAY,
    KAFKA_SERVER,
)

logger = logging.getLogger(__name__)

_producer: Optional[KafkaProducer] = None


def get_producer() -> KafkaProducer:
    """Retourne un producteur Kafka initialisé (singleton lazy)."""
    global _producer

    if _producer is not None:
        return _producer

    last_error: Exception | None = None

    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            logger.info(
                "Connexion Kafka (tentative %s/%s) -> %s",
                attempt,
                KAFKA_CONNECT_RETRIES,
                KAFKA_SERVER,
            )
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=50,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=10000,
            )
            # Force une requête metadata pour valider la connexion
            _producer.partitions_for("__consumer_offsets")
            logger.info("Connexion Kafka OK (%s)", KAFKA_SERVER)
            return _producer
        except (NoBrokersAvailable, KafkaError, OSError) as exc:
            last_error = exc
            logger.warning(
                "Kafka indisponible (%s). Nouvelle tentative dans %.1fs...",
                exc,
                KAFKA_CONNECT_RETRY_DELAY,
            )
            time.sleep(KAFKA_CONNECT_RETRY_DELAY)

    raise RuntimeError(
        f"Impossible de se connecter à Kafka ({KAFKA_SERVER}) "
        f"après {KAFKA_CONNECT_RETRIES} tentatives: {last_error}"
    )


def send_vote(topic: str, data: dict[str, Any]) -> None:
    """Envoie un vote JSON vers le topic Kafka.

    Args:
        topic: Nom du topic.
        data: Payload du vote.

    Raises:
        KafkaError: En cas d'échec d'envoi après retries internes.
    """
    producer = get_producer()
    key = data.get("vote_id")

    try:
        future = producer.send(topic, key=key, value=data)
        record = future.get(timeout=30)
        logger.debug(
            "Vote publié topic=%s partition=%s offset=%s vote_id=%s",
            topic,
            record.partition,
            record.offset,
            key,
        )
    except Exception:
        logger.exception("Échec d'envoi du vote %s vers %s", key, topic)
        raise


def close_producer() -> None:
    """Ferme proprement le producteur Kafka."""
    global _producer
    if _producer is not None:
        try:
            _producer.flush(timeout=10)
            _producer.close(timeout=10)
            logger.info("Producteur Kafka fermé")
        except Exception:
            logger.exception("Erreur lors de la fermeture du producteur Kafka")
        finally:
            _producer = None

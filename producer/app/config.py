"""Configuration du producteur Kafka."""

from __future__ import annotations

import os

KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "votes")
KAFKA_SERVER: str = os.getenv("KAFKA_SERVER", "localhost:9092")

# Intervalle entre deux votes (secondes)
VOTE_INTERVAL_SECONDS: float = float(os.getenv("VOTE_INTERVAL_SECONDS", "2"))

# Probabilité qu'un vote soit issu du Sénégal (sinon diaspora)
SENEGAL_VOTE_RATIO: float = float(os.getenv("SENEGAL_VOTE_RATIO", "0.80"))

# Tentatives de connexion Kafka au démarrage
KAFKA_CONNECT_RETRIES: int = int(os.getenv("KAFKA_CONNECT_RETRIES", "30"))
KAFKA_CONNECT_RETRY_DELAY: float = float(os.getenv("KAFKA_CONNECT_RETRY_DELAY", "2"))

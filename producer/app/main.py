"""Point d'entrée du producteur de votes électoraux."""

from __future__ import annotations

import logging
import signal
import sys
import time

from config import KAFKA_TOPIC, VOTE_INTERVAL_SECONDS
from generator import generate_vote
from kafka_client import close_producer, get_producer, send_vote

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")

_running = True


def _handle_signal(signum: int, _frame: object) -> None:
    """Arrêt gracieux sur SIGINT/SIGTERM."""
    global _running
    logger.info("Signal %s reçu — arrêt du producteur...", signum)
    _running = False


def main() -> None:
    """Boucle principale : génère et publie des votes en continu."""
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Démarrage du producteur | topic=%s | interval=%.1fs", KAFKA_TOPIC, VOTE_INTERVAL_SECONDS)

    get_producer()
    logger.info("Producteur Kafka prêt")

    sent = 0
    errors = 0

    while _running:
        started = time.perf_counter()
        try:
            vote = generate_vote()
            send_vote(KAFKA_TOPIC, vote)
            sent += 1

            location = (
                vote["region"]
                if vote["type"] == "SENEGAL"
                else f"{vote['continent']}/{vote['pays']}"
            )
            logger.info(
                "Vote envoyé #%s | type=%s | lieu=%s | candidat=%s | vote_id=%s",
                sent,
                vote["type"],
                location,
                vote["candidat"],
                vote["vote_id"],
            )
        except Exception:
            errors += 1
            logger.exception("Erreur lors de la génération/envoi (erreurs=%s)", errors)
            time.sleep(min(VOTE_INTERVAL_SECONDS * 2, 10))
            continue

        elapsed = time.perf_counter() - started
        sleep_for = max(0.0, VOTE_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)

    close_producer()
    logger.info("Producteur arrêté | envoyés=%s | erreurs=%s", sent, errors)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Arrêt fatal du producteur")
        close_producer()
        sys.exit(1)

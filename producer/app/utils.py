"""Utilitaires de chargement des données de référence."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DATA_PATH: str = os.getenv(
    "DATA_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"),
)


def load_json(filename: str) -> Any:
    """Charge un fichier JSON depuis le répertoire de données.

    Args:
        filename: Nom du fichier (ex. ``candidats.json``).

    Returns:
        Contenu JSON désérialisé.

    Raises:
        FileNotFoundError: Si le fichier est introuvable.
        json.JSONDecodeError: Si le JSON est invalide.
    """
    path = os.path.join(DATA_PATH, filename)
    logger.info("Chargement du fichier de référence: %s", path)

    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)

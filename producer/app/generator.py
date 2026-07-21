"""Générateur de votes simulés (Sénégal + diaspora)."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any

from faker import Faker

from config import SENEGAL_VOTE_RATIO
from utils import load_json

fake = Faker("fr_FR")

CANDIDATS: list[dict[str, Any]] = load_json("candidats.json")
PROFESSIONS: list[str] = load_json("professions.json")
CENTRES: dict[str, Any] = load_json("centres_vote.json")
ZONES_DIASPORA: dict[str, Any] = load_json("zone_diaspora.json")


def _now_iso() -> str:
    """Retourne l'horodatage UTC au format ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _fake_cni() -> str:
    """Génère un numéro CNI fictif."""
    return f"{random.randint(1, 2)}{random.randint(100000000, 999999999)}"


def _pick_candidat() -> str:
    """Retourne l'identifiant d'un candidat."""
    candidat = random.choice(CANDIDATS)
    return str(candidat["id"])


def random_senegal_vote() -> dict[str, Any]:
    """Génère un vote provenant d'un bureau au Sénégal."""
    profession = random.choice(PROFESSIONS)
    region = random.choice(list(CENTRES.keys()))
    departement = random.choice(list(CENTRES[region].keys()))
    centre_data = random.choice(CENTRES[region][departement])
    centre = centre_data["centre"]
    bureau = random.choice(centre_data["bureaux"])

    return {
        "vote_id": str(uuid.uuid4()),
        "timestamp": _now_iso(),
        "type": "SENEGAL",
        "cni": _fake_cni(),
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "age": random.randint(18, 80),
        "sexe": random.choice(["M", "F"]),
        "profession": profession,
        "region": region,
        "departement": departement,
        "centre": centre,
        "bureau": bureau,
        "continent": None,
        "pays": None,
        "ville": None,
        "candidat": _pick_candidat(),
    }


def random_diaspora_vote() -> dict[str, Any]:
    """Génère un vote provenant de la diaspora."""
    continent = random.choice(list(ZONES_DIASPORA.keys()))
    pays_data = random.choice(ZONES_DIASPORA[continent])
    pays = pays_data["pays"]
    ville = random.choice(list(pays_data["villes"].keys()))
    bureau = f"BV-{continent[:2].upper()}-{random.randint(1, 100)}"

    return {
        "vote_id": str(uuid.uuid4()),
        "timestamp": _now_iso(),
        "type": "DIASPORA",
        "cni": _fake_cni(),
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "age": random.randint(18, 80),
        "sexe": random.choice(["M", "F"]),
        "profession": random.choice(PROFESSIONS),
        "region": None,
        "departement": None,
        "centre": None,
        "bureau": bureau,
        "continent": continent,
        "pays": pays,
        "ville": ville,
        "candidat": _pick_candidat(),
    }


def generate_vote() -> dict[str, Any]:
    """Génère un vote Sénégal ou diaspora selon la ratio configurée."""
    if random.random() < SENEGAL_VOTE_RATIO:
        return random_senegal_vote()
    return random_diaspora_vote()

import random
import uuid
from datetime import datetime

from faker import Faker

from utils import load_json

fake = Faker()

CANDIDATS = load_json("candidats.json")
PROFESSIONS = load_json("professions.json")
CENTRES = load_json("centres_vote.json")
ZONES_DIASPORA = load_json("zone_diaspora.json")


def random_senegal_vote():

    region = random.choice(list(CENTRES.keys()))

    departement = random.choice(
        list(CENTRES[region].keys())
    )

    centre_data = random.choice(
        CENTRES[region][departement]
    )

    candidat = random.choice(CANDIDATS)

    return {

        "vote_id": str(uuid.uuid4()),

        "timestamp": datetime.utcnow().isoformat(),

        "type": "SENEGAL",

        "cni": fake.unique.numerify("###########"),

        "nom": fake.last_name(),

        "prenom": fake.first_name(),

        "age": random.randint(18, 80),

        "sexe": random.choice(["M", "F"]),

        "profession": random.choice(PROFESSIONS),

        "region": region,

        "departement": departement,

        "centre": centre_data["centre"],

        "bureau": random.choice(
            centre_data["bureaux"]
        ),

        "zone": None,

        "continent": None,

        "pays": None,

        "ville": None,

        "candidat": candidat["id"],
    }


def random_diaspora_vote():

    continent = random.choice(
        list(ZONES_DIASPORA.keys())
    )

    pays_data = random.choice(
        ZONES_DIASPORA[continent]
    )

    ville = random.choice(
        list(pays_data["villes"].keys())
    )

    candidat = random.choice(CANDIDATS)

    return {

        "vote_id": str(uuid.uuid4()),

        "timestamp": datetime.utcnow().isoformat(),

        "type": "DIASPORA",

        "cni": fake.unique.numerify("###########"),

        "nom": fake.last_name(),

        "prenom": fake.first_name(),

        "age": random.randint(18, 80),

        "sexe": random.choice(["M", "F"]),

        "profession": None,

        "region": None,

        "departement": None,

        "centre": None,

        "bureau": f"BV-{continent[:2].upper()}-{random.randint(1,100)}",

        "zone": continent,

        "continent": continent,

        "pays": pays_data["pays"],

        "ville": ville,

        "candidat": candidat["id"],
    }


def generate_vote():

    if random.random() < 0.8:
        return random_senegal_vote()

    return random_diaspora_vote()
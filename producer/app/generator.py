import random
from faker import Faker
from utils import load_json

fake = Faker()

# importations des donnees
CANDIDATS = load_json("candidats.json")
PROFESSIONS = load_json("professions.json")
CENTRES = load_json("centres_vote.json")
ZONES_DIASPORA = load_json("zone_diaspora.json")


def random_senegal_vote():
    profession = random.choice(PROFESSIONS)
    # Choix région
    region = random.choice(list(CENTRES.keys()))

    # Choix département
    departement = random.choice(list(CENTRES[region].keys()))

    # Choix centre
    centre_data = random.choice(CENTRES[region][departement])

    centre = centre_data["centre"]

    # Choix bureau dans ce centre
    bureau = random.choice(centre_data["bureaux"])

    # Choix candidat
    candidat = random.choice(CANDIDATS)

    return {
        "type": "SENEGAL",
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "age": random.randint(18, 80),
        "sexe": random.choice(["M", "F"]),
        "prefession" : profession,
        "region": region,
        "departement": departement,
        "centre": centre,
        "bureau": bureau,
        "candidat": candidat["id"],
    }


def random_diaspora_vote():
    zone = random.choice(list(ZONES_DIASPORA.keys()))
    pays_data = random.choice(ZONES_DIASPORA[zone])

    pays = pays_data["pays"]
    ville = random.choice(list(pays_data["villes"].keys()))
    bureau = f"BV-{zone[:2].upper()}-{random.randint(1, 100)}"

    candidat = random.choice(CANDIDATS)

    return {
        "type": "DIASPORA",
        "zone": zone,
        "pays": pays,
        "ville": ville,
        "nom": fake.last_name(),
        "prenom": fake.first_name(),
        "age": random.randint(18, 80),
        "sexe": random.choice(["M", "F"]),
        "bureau": bureau,
        "candidat": candidat["id"]
    }


def generate_vote():
    if random.random() < 0.80:
        return random_senegal_vote()
    else:
        return random_diaspora_vote()
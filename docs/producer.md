# Producer — schéma des votes et extension des référentiels

## Rôle

Le producteur Python simule en continu des votes électoraux et les publie sur Kafka (topic `KAFKA_TOPIC`, défaut `votes`).

## Modules

```text
producer/app/
├── main.py           # Boucle, logging, arrêt gracieux
├── generator.py      # Génération SENEGAL / DIASPORA
├── kafka_client.py   # KafkaProducer robuste (retries)
├── config.py         # Env vars
├── utils.py          # load_json
└── data/
    ├── candidats.json
    ├── centres_vote.json
    ├── zone_diaspora.json
    ├── professions.json
    ├── regions_senegal.json
    ├── departements.json
    └── bureaux_vote.json
```

Le volume Compose `./producer/app:/app` permet de modifier les JSON **sans rebuild** (redémarrage du conteneur recommandé pour recharger en mémoire).

## Configuration

| Variable | Défaut | Description |
| -------- | ------ | ----------- |
| `KAFKA_SERVER` | `kafka:9092` | Bootstrap |
| `KAFKA_TOPIC` | `votes` | Topic cible (**respecté** — bug historique corrigé) |
| `VOTE_INTERVAL_SECONDS` | `2` | Intervalle entre envois |
| `SENEGAL_VOTE_RATIO` | `0.80` | Probabilité d’un vote national |
| `KAFKA_CONNECT_RETRIES` | `30` | Tentatives de connexion au démarrage |
| `KAFKA_CONNECT_RETRY_DELAY` | `2` | Délai entre tentatives |

## Schéma JSON d’un vote

Aligné sur `spark/app/schemas.py` :

| Champ | Type | SENEGAL | DIASPORA |
| ----- | ---- | ------- | -------- |
| `vote_id` | string (UUID) | oui | oui |
| `timestamp` | ISO 8601 UTC (`…Z`) | oui | oui |
| `type` | `"SENEGAL"` \| `"DIASPORA"` | oui | oui |
| `cni` | string | oui | oui |
| `nom` / `prenom` | string | oui | oui |
| `age` | int 18–80 | oui | oui |
| `sexe` | `"M"` \| `"F"` | oui | oui |
| `profession` | string | oui | oui |
| `region` | string | oui | `null` |
| `departement` | string | oui | `null` |
| `centre` | string | oui | `null` |
| `bureau` | string | oui | oui (synthétique) |
| `continent` | string | `null` | oui |
| `pays` | string | `null` | oui |
| `ville` | string | `null` | oui |
| `candidat` | string (`id` JSON) | oui | oui |

### Exemple Sénégal

```json
{
  "vote_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-07-19T03:15:00.123Z",
  "type": "SENEGAL",
  "cni": "1987654321",
  "nom": "Diop",
  "prenom": "Awa",
  "age": 34,
  "sexe": "F",
  "profession": "Commerçant",
  "region": "Dakar",
  "departement": "Dakar-Plateau",
  "centre": "Lycée Lamine Guèye",
  "bureau": "BV-DAK-1",
  "continent": null,
  "pays": null,
  "ville": null,
  "candidat": "C003"
}
```

### Exemple diaspora

```json
{
  "vote_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "timestamp": "2026-07-19T03:15:02.456Z",
  "type": "DIASPORA",
  "cni": "2123456789",
  "nom": "Ndiaye",
  "prenom": "Ibrahima",
  "age": 41,
  "sexe": "M",
  "profession": "Ingénieur",
  "region": null,
  "departement": null,
  "centre": null,
  "bureau": "BV-EU-42",
  "continent": "Europe",
  "pays": "France",
  "ville": "Paris",
  "candidat": "C001"
}
```

## Publication Kafka

- Sérialisation JSON UTF-8
- Clé message = `vote_id`
- `acks=all`, retries internes
- Logging structuré : type, lieu, candidat, `vote_id`

## Ajouter un candidat

Éditer **`producer/app/data/candidats.json`** :

```json
{
  "id": "C007",
  "nom": "Nouveau",
  "prenom": "Candidat",
  "parti": "XYZ",
  "couleur": "#E67E22"
}
```

Le générateur tire `candidat["id"]` aléatoirement. Redémarrer le producer :

```bash
docker compose restart producer
```

Les agrégations Spark / le dashboard afficheront le nouvel `id` dès que des votes associés seront traités. Aucune migration PostgreSQL n’est requise (`candidat` est un `VARCHAR`).

## Ajouter une région / département / centre / bureau

Éditer **`producer/app/data/centres_vote.json`**. Structure hiérarchique :

```json
{
  "NomRégion": {
    "NomDépartement": [
      {
        "centre": "Nom du centre",
        "bureaux": ["BV-XXX-1", "BV-XXX-2"]
      }
    ]
  }
}
```

Exemple d’ajout :

```json
"Tambacounda": {
  "Bakel": [
    {
      "centre": "Lycée de Bakel",
      "bureaux": ["BV-BAK-1", "BV-BAK-2"]
    }
  ]
}
```

Puis :

```bash
docker compose restart producer
```

## Ajouter une zone diaspora

Éditer **`producer/app/data/zone_diaspora.json`** :

```json
"Amérique": [
  {
    "pays": "Canada",
    "villes": {
      "Montréal": 3,
      "Toronto": 2
    }
  }
]
```

Le champ top-level est le **continent** (plus `zone`) — aligné avec Spark `groupBy("continent", "pays")`.

## Ajouter une profession

Éditer `producer/app/data/professions.json` (liste de strings). Le champ s’appelle bien **`profession`** (typo historique `prefession` corrigée).

## Bugs historiques liés au schéma

1. Absence de `vote_id` / `timestamp` / `cni` → `clean_votes` filtrait **tous** les votes.
2. Typo `prefession` → profession toujours null.
3. Diaspora sur `zone` au lieu de `continent` → agrégations diaspora vides / incorrectes.
4. `KAFKA_TOPIC` ignoré → topic hardcodé.

Voir [changelog.md](changelog.md).

# PostgreSQL — schéma complet

## Connexion

| Paramètre | Valeur Compose |
| --------- | -------------- |
| Host (réseau Docker) | `postgres` |
| Port | `5432` |
| Base | `POSTGRES_DB` (défaut `election`) |
| User / password | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| Init | `postgres/init.sql` (monté en `:ro`) |
| Miroir doc | `postgres/schema.sql` (non utilisé pour l’init Docker) |
| Volume | `postgres_data` → `/var/lib/postgresql/data` |

URL JDBC Spark :

```text
jdbc:postgresql://postgres:5432/election
```

URL SQLAlchemy dashboard :

```text
postgresql+psycopg2://USER:PASSWORD@postgres:5432/election
```

## Table `votes_bruts` (append)

Stockage des votes enrichis. Spark écrit en **append** ; les agrégations relisent cette table et dédoublonnent sur `vote_id`.

| Colonne | Type | Description |
| ------- | ---- | ----------- |
| `vote_id` | `VARCHAR(64) NOT NULL` | Identifiant unique du vote |
| `timestamp` | `TIMESTAMP` | Horodatage du vote |
| `type` | `VARCHAR(20)` | `SENEGAL` ou `DIASPORA` |
| `cni` | `VARCHAR(32)` | Numéro CNI fictif |
| `nom` | `VARCHAR(100)` | Nom |
| `prenom` | `VARCHAR(100)` | Prénom |
| `age` | `INTEGER` | Âge (18–120 après clean) |
| `sexe` | `VARCHAR(10)` | `M` / `F` |
| `profession` | `VARCHAR(100)` | Profession |
| `region` | `VARCHAR(100)` | Région (Sénégal) |
| `departement` | `VARCHAR(100)` | Département |
| `centre` | `VARCHAR(150)` | Centre de vote |
| `bureau` | `VARCHAR(100)` | Bureau de vote |
| `continent` | `VARCHAR(100)` | Continent (diaspora) |
| `pays` | `VARCHAR(100)` | Pays (diaspora) |
| `ville` | `VARCHAR(100)` | Ville (diaspora) |
| `candidat` | `VARCHAR(20)` | Identifiant candidat (`C001`, …) |
| `ingestion_time` | `TIMESTAMP` | Instant d’ingestion Spark |
| `est_diaspora` | `BOOLEAN` | Dérivé de `type` |
| `tranche_age` | `VARCHAR(20)` | Tranche calculée |

### Index

- `idx_votes_bruts_candidat` (`candidat`)
- `idx_votes_bruts_type` (`type`)
- `idx_votes_bruts_region` (`region`)
- `idx_votes_bruts_timestamp` (`timestamp`)
- `idx_votes_bruts_vote_id` (`vote_id`)

## Tables d’agrégation (overwrite + truncate)

Toutes sont recalculées à chaque micro-batch non vide.

### `resultats_candidats`

| Colonne | Type |
| ------- | ---- |
| `candidat` | `VARCHAR(20)` |
| `nb_votes` | `BIGINT` |

### `resultats_regions`

| Colonne | Type |
| ------- | ---- |
| `region` | `VARCHAR(100)` |
| `nb_votes` | `BIGINT` |

Votes `type = SENEGAL` uniquement.

### `resultats_departements`

| Colonne | Type |
| ------- | ---- |
| `region` | `VARCHAR(100)` |
| `departement` | `VARCHAR(100)` |
| `nb_votes` | `BIGINT` |

### `resultats_bureaux`

| Colonne | Type |
| ------- | ---- |
| `bureau` | `VARCHAR(100)` |
| `nb_votes` | `BIGINT` |

### `resultats_diaspora`

| Colonne | Type |
| ------- | ---- |
| `continent` | `VARCHAR(100)` |
| `pays` | `VARCHAR(100)` |
| `nb_votes` | `BIGINT` |

> Correction : l’agrégation groupait autrefois sur un champ erroné `zone` au lieu de `continent`.

### `participation_sexe`

| Colonne | Type |
| ------- | ---- |
| `sexe` | `VARCHAR(10)` |
| `nb_votes` | `BIGINT` |

### `participation_age`

| Colonne | Type |
| ------- | ---- |
| `tranche_age` | `VARCHAR(20)` |
| `nb_votes` | `BIGINT` |

### `resultats_profession`

| Colonne | Type |
| ------- | ---- |
| `profession` | `VARCHAR(100)` |
| `nb_votes` | `BIGINT` |

## Modes d’écriture Spark

| Table | Mode JDBC | Option |
| ----- | --------- | ------ |
| `votes_bruts` | `append` | pas de truncate |
| Toutes les agrégations | `overwrite` | `truncate=true` (conserve le schéma) |

Options JDBC communes : `batchsize=1000`, `isolationLevel=READ_COMMITTED`.

## Requêtes de contrôle

```sql
SELECT COUNT(*) FROM votes_bruts;
SELECT candidat, nb_votes FROM resultats_candidats ORDER BY nb_votes DESC;
SELECT continent, pays, nb_votes FROM resultats_diaspora ORDER BY nb_votes DESC;
SELECT tranche_age, nb_votes FROM participation_age;
```

Via Docker :

```bash
docker exec -it postgres-election psql -U postgres -d election -c "SELECT COUNT(*) FROM votes_bruts;"
```

## PgAdmin

- URL : http://localhost:5050  
- Enregistrer le serveur : host `postgres`, port `5432`, base `election`, user/password du `.env`.

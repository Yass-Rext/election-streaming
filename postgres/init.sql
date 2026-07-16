CREATE TABLE IF NOT EXISTS resultats_candidats (
    candidat VARCHAR(20),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS resultats_regions (
    region VARCHAR(100),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS resultats_departements (
    region VARCHAR(100),
    departement VARCHAR(100),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS resultats_bureaux (
    bureau VARCHAR(100),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS resultats_diaspora (
    continent VARCHAR(100),
    pays VARCHAR(100),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS participation_sexe (
    sexe VARCHAR(10),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS participation_age (
    tranche_age VARCHAR(20),
    nb_votes BIGINT
);

CREATE TABLE IF NOT EXISTS resultats_profession (
    profession VARCHAR(100),
    nb_votes BIGINT
);
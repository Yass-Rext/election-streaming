-- ============================================================
-- Schéma PostgreSQL — Election Streaming
-- ============================================================

CREATE TABLE IF NOT EXISTS votes_bruts (
    vote_id         VARCHAR(64) NOT NULL,
    timestamp       TIMESTAMP,
    type            VARCHAR(20),
    cni             VARCHAR(32),
    nom             VARCHAR(100),
    prenom          VARCHAR(100),
    age             INTEGER,
    sexe            VARCHAR(10),
    profession      VARCHAR(100),
    region          VARCHAR(100),
    departement     VARCHAR(100),
    centre          VARCHAR(150),
    bureau          VARCHAR(100),
    continent       VARCHAR(100),
    pays            VARCHAR(100),
    ville           VARCHAR(100),
    candidat        VARCHAR(20),
    ingestion_time  TIMESTAMP,
    est_diaspora    BOOLEAN,
    tranche_age     VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_votes_bruts_candidat ON votes_bruts (candidat);
CREATE INDEX IF NOT EXISTS idx_votes_bruts_type ON votes_bruts (type);
CREATE INDEX IF NOT EXISTS idx_votes_bruts_region ON votes_bruts (region);
CREATE INDEX IF NOT EXISTS idx_votes_bruts_timestamp ON votes_bruts (timestamp);
CREATE INDEX IF NOT EXISTS idx_votes_bruts_vote_id ON votes_bruts (vote_id);

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

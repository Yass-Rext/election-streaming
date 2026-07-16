# 🗳️ Election Streaming Platform

> Pipeline de traitement et de visualisation des résultats d'une élection en temps réel avec **Apache Kafka**, **Apache Spark Structured Streaming**, **PostgreSQL**, **MinIO** et **Streamlit**.

---

# 📖 Présentation

Ce projet met en œuvre une architecture de **Data Engineering temps réel** permettant de simuler une élection nationale et de diffuser les résultats instantanément.

Les votes sont générés par un producteur Python, publiés dans Kafka, traités en continu par Spark Structured Streaming, stockés dans PostgreSQL et MinIO, puis visualisés dans un tableau de bord interactif Streamlit.

---

# 🎯 Objectifs

Le projet vise à :

* Simuler des votes provenant du Sénégal et de la diaspora.
* Transporter les votes en temps réel avec Kafka.
* Traiter les flux avec Spark Structured Streaming.
* Calculer automatiquement les résultats.
* Sauvegarder les votes bruts dans un Data Lake (MinIO).
* Alimenter une base PostgreSQL pour les tableaux de bord.
* Visualiser les résultats en temps réel avec Streamlit.

---

# 🏗️ Architecture

```text
                    +----------------------+
                    |  Python Producer     |
                    | (Simulation Votes)   |
                    +----------+-----------+
                               |
                               |
                               v
                    +----------------------+
                    |     Apache Kafka     |
                    |      Topic: votes    |
                    +----------+-----------+
                               |
                               |
                               v
             +-------------------------------------+
             | Spark Structured Streaming          |
             |                                     |
             | Parsing JSON                        |
             | Validation                          |
             | Nettoyage                           |
             | Agrégations                         |
             +---------------+---------------------+
                             |
              +--------------+---------------+
              |                              |
              |                              |
              v                              v
      PostgreSQL                     MinIO (Parquet)
              |                              |
              +--------------+---------------+
                             |
                             |
                             v
                  Dashboard Streamlit
```

---

# ⚙️ Technologies utilisées

| Technologie                | Rôle                     |
| -------------------------- | ------------------------ |
| Python 3.12                | Génération des votes     |
| Apache Kafka               | Message Broker           |
| Spark Structured Streaming | Traitement temps réel    |
| PostgreSQL                 | Stockage des agrégations |
| MinIO                      | Data Lake                |
| Streamlit                  | Dashboard                |
| Docker                     | Conteneurisation         |
| Docker Compose             | Orchestration            |
| Faker                      | Génération de données    |
| SQLAlchemy                 | Connexion PostgreSQL     |
| Plotly                     | Visualisation            |

---

# 📁 Structure du projet

```text
election-streaming/
│
├── docker-compose.yml
├── .env
├── README.md
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py
│       ├── generator.py
│       ├── kafka_client.py
│       ├── config.py
│       ├── utils.py
│       └── data/
│
├── spark/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── app.py
│       ├── config.py
│       ├── schema.py
│       ├── transformations.py
│       ├── aggregations.py
│       └── sinks/
│           ├── postgres.py
│           └── minio.py
│
├── postgres/
│   └── init.sql
│
├── dashboard/
│   ├── Dockerfile
│   ├── Home.py
│   ├── requirements.txt
│   ├── database.py
│   └── pages/
│
└── docs/
```

---

# 🔄 Fonctionnement du pipeline

## 1. Génération des votes

Le producteur Python génère en continu des votes réalistes.

Chaque vote contient notamment :

* Identifiant
* Horodatage
* Nom
* Prénom
* Sexe
* Âge
* Profession
* Région
* Département
* Centre
* Bureau
* Candidat
* Pays (diaspora)

Les données sont produites grâce aux fichiers JSON de référence.

---

## 2. Publication Kafka

Chaque vote est envoyé dans le topic :

```text
votes
```

Le producteur sérialise les données au format JSON.

---

## 3. Traitement Spark

Spark Structured Streaming :

* lit le topic Kafka ;
* convertit le JSON en DataFrame ;
* valide les données ;
* enrichit les votes ;
* calcule les agrégations en temps réel.

Les principales agrégations sont :

* Résultats par candidat
* Résultats par région
* Résultats par département
* Résultats par bureau
* Résultats de la diaspora
* Participation par sexe
* Participation par tranche d'âge
* Participation par profession

---

## 4. Stockage

### PostgreSQL

Les agrégations sont enregistrées dans PostgreSQL afin d'alimenter le tableau de bord.

### MinIO

Les votes bruts sont archivés au format Parquet dans un bucket MinIO.

Cette approche permet de constituer un Data Lake pour des traitements analytiques futurs.

---

# 🗄️ Tables PostgreSQL

Le projet crée automatiquement les tables suivantes :

* resultats_candidats
* resultats_regions
* resultats_departements
* resultats_bureaux
* resultats_diaspora
* participation_sexe
* participation_age
* resultats_profession

---

# ▶️ Lancement en local

## 1. Cloner le projet

```bash
git clone https://github.com/<votre-utilisateur>/election-streaming.git
cd election-streaming
```

## 2. Démarrer les services

```bash
docker compose up --build
```

## 3. Vérifier les interfaces

| Service       | URL                   |
| ------------- | --------------------- |
| Kafka UI      | http://localhost:8080 |
| Spark UI      | http://localhost:8081 |
| Streamlit     | http://localhost:8501 |
| MinIO Console | http://localhost:9001 |
| PgAdmin       | http://localhost:5050 |

---

# 📊 Dashboard

Le tableau de bord permet de consulter :

* Résultats par candidat
* Votes par région
* Votes par département
* Votes par bureau
* Répartition de la diaspora
* Participation par sexe
* Participation par tranche d'âge

Les données sont mises à jour automatiquement à partir de PostgreSQL.

---

# 🚀 Améliorations possibles

* Détection des fraudes électorales.
* Déduplication des votes.
* Authentification des électeurs.
* Intégration avec Apache Airflow.
* Historisation des résultats.
* Déploiement Kubernetes.
* Surveillance avec Prometheus et Grafana.
* Alertes en temps réel.

---

# 👨‍💻 Auteur

**Mamadou Yassarou Diallo**

Projet réalisé dans le cadre d'un projet de Data Engineering.

---

# 📜 Licence

Ce projet est distribué à des fins pédagogiques et de démonstration.

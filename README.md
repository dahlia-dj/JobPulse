# Plateforme Big Data — Collecte et analyse des offres d'emploi

PFE Master 2 Data Engineering. Pipeline : Collecte (France Travail, Adzuna) →
Ingestion Bronze (MinIO) → Traitement Silver/Gold (PySpark, à venir) →
Visualisation (PostgreSQL + Power BI).

Ce dépôt contient pour l'instant les **Phases 1 et 2** : l'environnement
d'infrastructure (Docker Compose) et les connecteurs de collecte.

## 1. Prérequis

- Docker et Docker Compose v2 (`docker compose version`)
- Un compte développeur [France Travail](https://francetravail.io) (client_id / client_secret)
- Un compte développeur [Adzuna](https://developer.adzuna.com) (app_id / app_key)

## 2. Installation

```bash
# 1. Copier le fichier d'environnement et le compléter
cp .env.example .env

# 2. Générer une clé Fernet pour Airflow et la coller dans .env
python3 scripts/generate_fernet_key.py
# → copier le résultat dans AIRFLOW__CORE__FERNET_KEY= (dans .env)

# 3. (Linux/Mac uniquement) renseigner votre UID pour éviter les problèmes
#    de permissions sur les volumes montés
echo "AIRFLOW_UID=$(id -u)" >> .env

# 4. Renseigner dans .env :
#    - POSTGRES_PASSWORD, MINIO_ROOT_PASSWORD (mots de passe de votre choix)
#    - FRANCE_TRAVAIL_CLIENT_ID / FRANCE_TRAVAIL_CLIENT_SECRET
#    - ADZUNA_APP_ID / ADZUNA_APP_KEY

# 5. Construire et démarrer tous les services
docker compose up --build -d

# 6. Suivre le démarrage (le premier lancement prend quelques minutes :
#    build de l'image Airflow, migration de la base, création des buckets)
docker compose logs -f airflow-init minio-init
```

## 3. Accès aux interfaces

| Service            | URL                          | Identifiants                          |
|---------------------|-------------------------------|----------------------------------------|
| Airflow             | http://localhost:8080        | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (.env) |
| Console MinIO       | http://localhost:9001        | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (.env) |
| PostgreSQL (DWH)    | localhost:5432 (base `job_offers_dwh`) | `POSTGRES_USER` / `POSTGRES_PASSWORD` (.env) |

## 4. Vérifier que tout fonctionne

```bash
# Tous les conteneurs doivent être "healthy" ou "running"
docker compose ps

# Les 2 buckets doivent apparaître dans la console MinIO (http://localhost:9001) :
#   - job-offers-bronze
#   - job-offers-silver

# Les tables du schéma Gold doivent exister :
docker compose exec postgres psql -U $POSTGRES_USER -d job_offers_dwh -c "\dt gold.*"
```

## 5. Lancer la collecte

Dans l'interface Airflow (http://localhost:8080), activer puis déclencher
manuellement le DAG **`collect_job_offers`** (icône ▶). Il exécute en
parallèle l'extraction France Travail et Adzuna, valide que des données ont
bien été reçues, puis les écrit dans MinIO sous :

```
job-offers-bronze/
├── france_travail/date=AAAA-MM-JJ/france_travail_AAAAMMJJ_HHMMSS.json
└── adzuna/date=AAAA-MM-JJ/adzuna_AAAAMMJJ_HHMMSS.json
```

## 6. Structure du projet

```
.
├── docker-compose.yml          # Orchestration de tous les services
├── .env.example                # Variables d'environnement (à copier en .env)
├── airflow/
│   ├── Dockerfile              # Image Airflow + dépendances du projet
│   ├── requirements.txt
│   ├── dags/
│   │   └── collect_job_offers.py   # DAG de collecte quotidienne
│   ├── plugins/
│   │   ├── sources/
│   │   │   ├── france_travail.py   # Connecteur API France Travail
│   │   │   └── adzuna.py           # Connecteur API Adzuna
│   │   └── storage/
│   │       └── minio_client.py     # Écriture JSON vers MinIO (Bronze)
│   └── logs/
├── postgres/
│   └── init/                   # Scripts exécutés au 1er démarrage de Postgres
│       ├── 01_create_dwh_database.sh   # Crée la base job_offers_dwh
│       └── 02_create_gold_schema.sh    # Crée le schéma en étoile Gold
└── scripts/
    └── generate_fernet_key.py
```

## 7. Prochaines étapes (à venir)

- Couche Silver : jobs PySpark de nettoyage, déduplication, extraction de
  compétences (spaCy).
- Couche Gold : agrégations et chargement dans les tables `gold.*` déjà créées.
- Connexion Power BI à PostgreSQL pour les tableaux de bord.

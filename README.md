# Plateforme Big Data — Collecte et analyse des offres d'emploi

PFE Master 2 Data Engineering. Pipeline : Collecte (France Travail, Adzuna,
HelloWork, Free-Work) → Bronze (JSON dans MinIO) → Silver (PySpark + spaCy,
Parquet dans MinIO) → Gold (schéma en étoile dans PostgreSQL) → Visualisation
(Power BI).

Les trois DAG Airflow s'enchaînent chaque jour : `collect_job_offers` (06:00),
`silver_job_offers` (07:00), puis `gold_job_offers`, déclenché automatiquement
dès que la couche Silver est prête.

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
| Application JobPulse (Streamlit) | http://localhost:8501 | aucun |

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
manuellement le DAG **`collect_job_offers`**. Il exécute en
parallèle l'extraction France Travail et Adzuna, valide que des données ont
bien été reçues, puis les écrit dans MinIO sous :

```
job-offers-bronze/
├── france_travail/date=AAAA-MM-JJ/france_travail_AAAAMMJJ_HHMMSS.json
└── adzuna/date=AAAA-MM-JJ/adzuna_AAAAMMJJ_HHMMSS.json
```

## 5 bis. Couches Silver et Gold

1. Déclencher le DAG **`silver_job_offers`** (ou attendre 07:00). Il lit tout le
   Bronze, nettoie, dédoublonne, enrichit (spaCy) et écrit
   `job-offers-silver/job_offers/*.parquet`.
2. Quand son contrôle qualité réussit, Airflow lance tout seul
   **`gold_job_offers`** : le Parquet est transformé en schéma en étoile et
   chargé dans PostgreSQL (`job_offers_dwh`, schéma `gold`).
3. Vérifier le chargement :

```bash
docker exec -it pfe_postgres psql -U <POSTGRES_USER> -d job_offers_dwh -c "SELECT * FROM gold.etl_runs ORDER BY run_id DESC LIMIT 3;"
```

L'application Streamlit (http://localhost:8501) lit ces tables Gold : elle
fonctionne dès que `gold_job_offers` a tourné une première fois.

Détail du modèle : `docs/gold_schema_etoile.md`. Connexion Power BI et mesures
DAX : `powerbi/MODELE_ET_MESURES_DAX.md`.

## 6. Structure du projet

```
.
├── docker-compose.yml          # Orchestration de tous les services
├── .env.example                # Variables d'environnement (à copier en .env)
├── airflow/
│   ├── Dockerfile              # Image Airflow + dépendances du projet
│   ├── requirements.txt
│   ├── dags/
│   │   ├── collect_job_offers_bronze.py # DAG collect_job_offers (Bronze)
│   │   ├── bronze_to_silver.py          # DAG silver_job_offers (Silver)
│   │   └── silver_to_gold.py            # DAG gold_job_offers (Gold)
│   ├── plugins/
│   │   ├── .airflowignore              # empêche Airflow de charger silver/ et gold/ comme plugins
│   │   ├── sources/                    # connecteurs France Travail, Adzuna, HelloWork, Free-Work
│   │   ├── storage/
│   │   │   └── minio_client.py         # Écriture JSON vers MinIO (Bronze)
│   │   ├── silver/                     # parsing, transform, enrich (spaCy), job
│   │   └── gold/                       # schema (SQL), model (PySpark), job (chargement)
│   └── logs/
├── postgres/
│   └── init/                   # Scripts exécutés au 1er démarrage de Postgres
│       ├── 01_create_dwh_database.sh   # Crée la base job_offers_dwh
│       └── 02_create_gold_schema.sh    # Crée les schémas gold et gold_staging (vides)
├── streamlit/                  # Application web (Streamlit)
│   ├── app.py                  # pages : tableau de bord, Mon Profil Matcher, favoris
│   ├── db.py                   # requêtes SQL vers le schéma gold (lecture seule)
│   ├── charts.py               # graphiques Plotly (carte de France, barres...)
│   ├── cv.py                   # lecture d'un CV PDF -> compétences (catalogue spaCy de silver/)
│   ├── assets/                 # logo, contours des régions (GeoJSON)
│   ├── .streamlit/config.toml  # thème sombre
│   ├── requirements.txt
│   └── Dockerfile
├── docs/
│   ├── data_dictionary.md
│   └── gold_schema_etoile.md   # Modèle Gold : tables, vues, chargement
├── powerbi/
│   └── MODELE_ET_MESURES_DAX.md  # Connexion, relations et mesures Power BI
└── scripts/
    └── generate_fernet_key.py
```

## 7. Prochaines étapes

- Tableaux de bord Power BI connectés à `job_offers_dwh` (voir `powerbi/`).
- Chaînage de la collecte vers Silver par Dataset, comme Silver → Gold.

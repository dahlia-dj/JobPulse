#!/bin/bash
# Exécuté automatiquement par l'image postgres au premier démarrage
# (dossier /docker-entrypoint-initdb.d), après 01_create_dwh_database.sh.
#
# Crée seulement les schémas de l'entrepôt :
#   - gold         : schéma en étoile lu par Power BI ;
#   - gold_staging : tables de transit utilisées pendant un chargement.
#
# Les tables, index et vues ne sont PAS créés ici : c'est le job Gold
# (airflow/plugins/gold/schema.py, appelé par le DAG gold_job_offers) qui les
# crée et les tient à jour, pour qu'il n'existe qu'une seule définition du schéma.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DWH_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS gold;
    CREATE SCHEMA IF NOT EXISTS gold_staging;
EOSQL

echo "Schémas 'gold' et 'gold_staging' prêts dans '${POSTGRES_DWH_DB}'."
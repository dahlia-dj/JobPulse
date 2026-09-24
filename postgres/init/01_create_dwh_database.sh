#!/bin/bash
# Exécuté automatiquement par l'image postgres au premier démarrage
# (dossier /docker-entrypoint-initdb.d). Crée la base de l'entrepôt Gold,
# séparée de la base de métadonnées Airflow (POSTGRES_DB).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE "${POSTGRES_DWH_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_DWH_DB}')\gexec
EOSQL

echo "Base '${POSTGRES_DWH_DB}' prête (entrepôt Gold)."

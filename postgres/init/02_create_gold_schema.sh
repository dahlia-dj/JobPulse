#!/bin/bash
# Crée le schéma "gold" et un premier jeu de tables (dimensions + faits)
# dans la base DWH nouvellement créée. Ce schéma sera alimenté plus tard
# par les jobs PySpark de la couche Gold — il sert ici de socle de départ.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DWH_DB" <<-EOSQL

    CREATE SCHEMA IF NOT EXISTS gold;

    -- Dimension : date de publication de l'offre
    CREATE TABLE IF NOT EXISTS gold.dim_date (
        date_id         INTEGER PRIMARY KEY,      -- format AAAAMMJJ
        full_date       DATE NOT NULL,
        year            SMALLINT NOT NULL,
        month           SMALLINT NOT NULL,
        month_name      VARCHAR(20) NOT NULL,
        week_of_year    SMALLINT NOT NULL,
        day_of_week     SMALLINT NOT NULL
    );

    -- Dimension : localisation de l'offre
    CREATE TABLE IF NOT EXISTS gold.dim_location (
        location_id     SERIAL PRIMARY KEY,
        city            VARCHAR(150),
        department_code VARCHAR(10),
        department_name VARCHAR(150),
        region          VARCHAR(150),
        country         VARCHAR(100) DEFAULT 'France'
    );

    -- Dimension : entreprise
    CREATE TABLE IF NOT EXISTS gold.dim_company (
        company_id      SERIAL PRIMARY KEY,
        company_name    VARCHAR(255) NOT NULL,
        sector          VARCHAR(150)
    );

    -- Dimension : compétence (extraite via spaCy en couche Silver)
    CREATE TABLE IF NOT EXISTS gold.dim_skill (
        skill_id        SERIAL PRIMARY KEY,
        skill_name      VARCHAR(150) NOT NULL UNIQUE,
        skill_category  VARCHAR(100)
    );

    -- Dimension : source de la donnée
    CREATE TABLE IF NOT EXISTS gold.dim_source (
        source_id       SERIAL PRIMARY KEY,
        source_name     VARCHAR(100) NOT NULL UNIQUE  -- france_travail | adzuna | scraping
    );

    -- Table de faits : une ligne par offre d'emploi dédupliquée
    CREATE TABLE IF NOT EXISTS gold.fact_job_offer (
        offer_id            BIGSERIAL PRIMARY KEY,
        source_offer_id     VARCHAR(255) NOT NULL,   -- identifiant chez la source d'origine
        date_id             INTEGER REFERENCES gold.dim_date(date_id),
        location_id         INTEGER REFERENCES gold.dim_location(location_id),
        company_id          INTEGER REFERENCES gold.dim_company(company_id),
        source_id           INTEGER REFERENCES gold.dim_source(source_id),
        job_title           VARCHAR(255) NOT NULL,
        contract_type       VARCHAR(50),
        salary_min          NUMERIC(10,2),
        salary_max          NUMERIC(10,2),
        experience_required VARCHAR(100),
        created_at          TIMESTAMP DEFAULT now()
    );

    -- Table de liaison faits <-> compétences (relation many-to-many)
    CREATE TABLE IF NOT EXISTS gold.fact_job_offer_skill (
        offer_id        BIGINT REFERENCES gold.fact_job_offer(offer_id) ON DELETE CASCADE,
        skill_id        INTEGER REFERENCES gold.dim_skill(skill_id) ON DELETE CASCADE,
        PRIMARY KEY (offer_id, skill_id)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_job_offer_date ON gold.fact_job_offer(date_id);
    CREATE INDEX IF NOT EXISTS idx_fact_job_offer_location ON gold.fact_job_offer(location_id);
    CREATE INDEX IF NOT EXISTS idx_fact_job_offer_source_offer_id ON gold.fact_job_offer(source_offer_id);

    INSERT INTO gold.dim_source (source_name) VALUES
        ('france_travail'), ('adzuna'), ('scraping')
    ON CONFLICT (source_name) DO NOTHING;

EOSQL

echo "Schéma 'gold' initialisé dans '${POSTGRES_DWH_DB}'."

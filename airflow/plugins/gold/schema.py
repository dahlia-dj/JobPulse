"""
Schéma en étoile de la couche Gold (PostgreSQL, base job_offers_dwh, schéma `gold`).

- SCHEMA_VERSION : si la base contient une autre version (ou l'ancien squelette
  créé par postgres/init/02_create_gold_schema.sh), le schéma `gold` est recréé.
- DDL : tables (dimensions, faits, table de liaison) et index.
- VIEWS : vues prêtes pour Power BI : gold_top_skills, gold_repartition_geographique,
  gold_statistiques_salaires, gold_tendances_recrutement (cahier des charges) et v_offers.
- LOAD_ORDER : ordre de chargement (dimensions -> faits -> liaison) imposé par
  les clés étrangères.
"""

SCHEMA_VERSION = 3  # 2 : vues du cahier des charges ; 3 : coordonnées dans dim_location

DDL = """
CREATE TABLE gold.schema_version (version INTEGER NOT NULL);

-- ---------------------------------------------------------------- dimensions
CREATE TABLE gold.dim_date (
    date_key        INTEGER PRIMARY KEY,            -- AAAAMMJJ
    full_date       DATE NOT NULL UNIQUE,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,              -- 1 = lundi ... 7 = dimanche
    day_name        VARCHAR(10) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         SMALLINT NOT NULL,
    year            SMALLINT NOT NULL,
    year_month      CHAR(7) NOT NULL,               -- AAAA-MM
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE gold.dim_source (
    source_key      SMALLINT PRIMARY KEY,
    source_code     VARCHAR(30) NOT NULL UNIQUE,
    source_name     VARCHAR(50) NOT NULL,
    access_method   VARCHAR(20) NOT NULL            -- API / Scraping
);

CREATE TABLE gold.dim_contract (
    contract_key    SMALLINT PRIMARY KEY,
    contract_type   VARCHAR(20) NOT NULL,
    contract_label  VARCHAR(40) NOT NULL,
    work_time       VARCHAR(20) NOT NULL,
    work_time_label VARCHAR(40) NOT NULL,
    UNIQUE (contract_type, work_time)
);

CREATE TABLE gold.dim_job_family (
    job_family_key   SMALLINT PRIMARY KEY,
    job_family_code  VARCHAR(30) NOT NULL UNIQUE,
    job_family_label VARCHAR(60) NOT NULL,
    in_scope         BOOLEAN NOT NULL               -- faux pour HORS_PERIMETRE
);

CREATE TABLE gold.dim_experience (
    experience_key   SMALLINT PRIMARY KEY,
    experience_level VARCHAR(20) NOT NULL UNIQUE,
    experience_label VARCHAR(40) NOT NULL,
    sort_order       SMALLINT NOT NULL
);

CREATE TABLE gold.dim_education (
    education_key    SMALLINT PRIMARY KEY,
    education_level  VARCHAR(20) NOT NULL UNIQUE,
    education_label  VARCHAR(40) NOT NULL,
    sort_order       SMALLINT NOT NULL
);

CREATE TABLE gold.dim_location (
    location_key     BIGINT PRIMARY KEY,            -- empreinte (ville, département, région)
    city             VARCHAR(150),
    department_code  VARCHAR(5),
    region           VARCHAR(80) NOT NULL,          -- « Non renseignée » si inconnue
    country          VARCHAR(30) NOT NULL,
    latitude         DOUBLE PRECISION,              -- coordonnées de la ville (cartes), si connues
    longitude        DOUBLE PRECISION
);

CREATE TABLE gold.dim_company (
    company_key      BIGINT PRIMARY KEY,            -- empreinte du nom
    company_name     VARCHAR(255) NOT NULL
);

CREATE TABLE gold.dim_skill (
    skill_key        SMALLINT PRIMARY KEY,
    skill_name       VARCHAR(80) NOT NULL UNIQUE,
    skill_category   VARCHAR(60) NOT NULL
);

-- ---------------------------------------------------------------- faits
CREATE TABLE gold.fact_job_offer (
    offer_key            BIGINT PRIMARY KEY,         -- empreinte de offer_id (stable d'un chargement à l'autre)
    offer_id             CHAR(40) NOT NULL UNIQUE,   -- identifiant Silver (SHA-1)
    source_key           SMALLINT NOT NULL REFERENCES gold.dim_source,
    contract_key         SMALLINT NOT NULL REFERENCES gold.dim_contract,
    job_family_key       SMALLINT NOT NULL REFERENCES gold.dim_job_family,
    experience_key       SMALLINT NOT NULL REFERENCES gold.dim_experience,
    education_key        SMALLINT NOT NULL REFERENCES gold.dim_education,
    location_key         BIGINT   NOT NULL REFERENCES gold.dim_location,
    company_key          BIGINT   NOT NULL REFERENCES gold.dim_company,
    date_posted_key      INTEGER  NOT NULL REFERENCES gold.dim_date,
    first_seen_key       INTEGER  NOT NULL REFERENCES gold.dim_date,
    last_seen_key        INTEGER  NOT NULL REFERENCES gold.dim_date,
    title                TEXT NOT NULL,
    url                  TEXT,
    salary_annual_min    NUMERIC(10,2),
    salary_annual_max    NUMERIC(10,2),
    salary_annual_avg    NUMERIC(10,2),
    experience_years_min SMALLINT,
    skills_count         SMALLINT NOT NULL,
    snapshots_seen       SMALLINT NOT NULL,
    days_online          INTEGER NOT NULL,           -- last_seen - first_seen + 1
    is_active            BOOLEAN NOT NULL,
    is_merged            BOOLEAN NOT NULL,
    contract_inferred    BOOLEAN NOT NULL,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Une offre a plusieurs compétences : table de liaison (relation plusieurs-à-plusieurs)
CREATE TABLE gold.bridge_offer_skill (
    offer_key   BIGINT   NOT NULL REFERENCES gold.fact_job_offer ON DELETE CASCADE,
    skill_key   SMALLINT NOT NULL REFERENCES gold.dim_skill,
    PRIMARY KEY (offer_key, skill_key)
);

-- Journal des chargements (date de dernière mise à jour affichable dans Power BI)
CREATE TABLE gold.etl_runs (
    run_id        SERIAL PRIMARY KEY,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    silver_rows   INTEGER NOT NULL,
    fact_rows     INTEGER NOT NULL,
    bridge_rows   INTEGER NOT NULL
);

CREATE INDEX idx_fact_source      ON gold.fact_job_offer (source_key);
CREATE INDEX idx_fact_contract    ON gold.fact_job_offer (contract_key);
CREATE INDEX idx_fact_family      ON gold.fact_job_offer (job_family_key);
CREATE INDEX idx_fact_location    ON gold.fact_job_offer (location_key);
CREATE INDEX idx_fact_company     ON gold.fact_job_offer (company_key);
CREATE INDEX idx_fact_date_posted ON gold.fact_job_offer (date_posted_key);
CREATE INDEX idx_bridge_skill     ON gold.bridge_offer_skill (skill_key);
"""

# Vues : par défaut, seules les offres du périmètre (in_scope) sont comptées.
# Les quatre vues gold_* correspondent aux vues demandées dans le cahier des charges.
VIEWS = """
-- Table à plat : une ligne par offre avec tous les libellés (exploration, contrôles)
CREATE OR REPLACE VIEW gold.v_offers AS
SELECT f.offer_id, f.title, c.company_name, l.city, l.department_code, l.region,
       s.source_name, ct.contract_label, ct.work_time_label,
       jf.job_family_label, jf.in_scope, e.experience_label, ed.education_label,
       f.salary_annual_min, f.salary_annual_max, f.salary_annual_avg, f.experience_years_min,
       f.skills_count, dp.full_date AS date_posted, fs.full_date AS first_seen, ls.full_date AS last_seen,
       f.days_online, f.snapshots_seen, f.is_active, f.is_merged, f.url
FROM gold.fact_job_offer f
JOIN gold.dim_company c     ON c.company_key = f.company_key
JOIN gold.dim_location l    ON l.location_key = f.location_key
JOIN gold.dim_source s      ON s.source_key = f.source_key
JOIN gold.dim_contract ct   ON ct.contract_key = f.contract_key
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
JOIN gold.dim_experience e  ON e.experience_key = f.experience_key
JOIN gold.dim_education ed  ON ed.education_key = f.education_key
JOIN gold.dim_date dp       ON dp.date_key = f.date_posted_key
JOIN gold.dim_date fs       ON fs.date_key = f.first_seen_key
JOIN gold.dim_date ls       ON ls.date_key = f.last_seen_key;

-- Vue « Top Compétences » : métier | compétence | nombre d'offres | % d'apparition dans le métier
CREATE OR REPLACE VIEW gold.gold_top_skills AS
WITH offres_metier AS (
    SELECT f.job_family_key, COUNT(*) AS nb_offres_metier
    FROM gold.fact_job_offer f GROUP BY f.job_family_key
)
SELECT jf.job_family_label                                   AS metier,
       sk.skill_name                                         AS competence,
       sk.skill_category                                     AS categorie,
       COUNT(*)                                              AS nb_offres,
       ROUND(100.0 * COUNT(*) / om.nb_offres_metier, 1)      AS pourcentage_apparition,
       RANK() OVER (PARTITION BY jf.job_family_label ORDER BY COUNT(*) DESC) AS rang
FROM gold.bridge_offer_skill b
JOIN gold.fact_job_offer f  ON f.offer_key = b.offer_key
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
JOIN gold.dim_skill sk      ON sk.skill_key = b.skill_key
JOIN offres_metier om       ON om.job_family_key = f.job_family_key
WHERE jf.in_scope
GROUP BY jf.job_family_label, sk.skill_name, sk.skill_category, om.nb_offres_metier;

-- Vue « Cartographie » : région | ville | nombre d'offres actives
CREATE OR REPLACE VIEW gold.gold_repartition_geographique AS
-- (regroupement par région et ville seulement : le département n'est fourni que par France Travail)
SELECT l.region,
       COALESCE(l.city, 'Ville non précisée')                AS ville,
       COUNT(*)                                              AS nb_offres_actives
FROM gold.fact_job_offer f
JOIN gold.dim_location l    ON l.location_key = f.location_key
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
WHERE jf.in_scope AND f.is_active
GROUP BY l.region, COALESCE(l.city, 'Ville non précisée');

-- Vue « Grille Salariale » : métier | contrat | expérience | salaire min moyen | max moyen | médian
-- (offres avec un salaire annualisé uniquement ; nb_offres indique la fiabilité de chaque ligne)
CREATE OR REPLACE VIEW gold.gold_statistiques_salaires AS
SELECT jf.job_family_label                                   AS metier,
       ct.contract_label                                     AS type_contrat,
       e.experience_label                                    AS experience,
       COUNT(*)                                              AS nb_offres,
       ROUND(AVG(f.salary_annual_min), 0)                    AS salaire_min_moyen,
       ROUND(AVG(f.salary_annual_max), 0)                    AS salaire_max_moyen,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.salary_annual_avg)::numeric, 0) AS salaire_median
FROM gold.fact_job_offer f
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
JOIN gold.dim_contract ct   ON ct.contract_key = f.contract_key
JOIN gold.dim_experience e  ON e.experience_key = f.experience_key
WHERE jf.in_scope AND f.salary_annual_avg IS NOT NULL
GROUP BY jf.job_family_label, ct.contract_label, e.experience_label;

-- Vue « Évolution Temporelle » : mois de publication | métier | nombre d'offres publiées
CREATE OR REPLACE VIEW gold.gold_tendances_recrutement AS
SELECT d.year_month                                          AS mois_publication,
       jf.job_family_label                                   AS metier,
       COUNT(*)                                              AS nb_offres_publiees
FROM gold.fact_job_offer f
JOIN gold.dim_date d        ON d.date_key = f.date_posted_key
JOIN gold.dim_job_family jf ON jf.job_family_key = f.job_family_key
WHERE jf.in_scope
GROUP BY d.year_month, jf.job_family_label;
"""

# Ordre de chargement (les clés étrangères imposent dimensions -> faits -> liaison).
LOAD_ORDER = [
    "dim_date", "dim_source", "dim_contract", "dim_job_family", "dim_experience",
    "dim_education", "dim_location", "dim_company", "dim_skill",
    "fact_job_offer", "bridge_offer_skill",
]

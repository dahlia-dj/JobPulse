# Couche Gold — schéma en étoile (PostgreSQL)

La couche Gold transforme la table Silver (Parquet dans MinIO, une ligne par offre unique, 36 colonnes)
en un **schéma en étoile** dans PostgreSQL : base `job_offers_dwh`, schéma `gold`.
Elle est lue par Power BI et par l'application Streamlit.

## 1. Fichiers

| Fichier | Rôle |
|---|---|
| `airflow/dags/silver_to_gold.py` | DAG `gold_job_offers` : `build_gold_layer` puis `check_quality`. Déclenché automatiquement quand le DAG Silver publie le Dataset `s3://job-offers-silver/job_offers`. |
| `airflow/dags/bronze_to_silver.py` | DAG `silver_job_offers` : sa tâche `check_quality` publie ce Dataset (seulement si le contrôle qualité Silver réussit). |
| `airflow/plugins/gold/schema.py` | Définition SQL : tables, index, vues, numéro de version du schéma (`SCHEMA_VERSION`). |
| `airflow/plugins/gold/model.py` | Construction des dimensions, de la table de faits et de la table de liaison avec PySpark. |
| `airflow/plugins/gold/job.py` | Orchestration : téléchargement du Parquet, Spark, écriture JDBC, bascule transactionnelle, rapport. |
| `airflow/Dockerfile` | Ajoute le pilote JDBC PostgreSQL (`/opt/spark-jars/postgresql-42.7.4.jar`). |
| `postgres/init/01_create_dwh_database.sh` | Crée la base `job_offers_dwh` au premier démarrage de PostgreSQL. |
| `postgres/init/02_create_gold_schema.sh` | Crée les schémas vides `gold` et `gold_staging` au premier démarrage. |

## 2. Déroulement d'un chargement

1. Téléchargement du Parquet Silver (`job-offers-silver/job_offers/*.parquet`).
2. Création ou mise à jour du schéma : si `gold.schema_version` est absente ou différente de
   `SCHEMA_VERSION`, le schéma `gold` est supprimé puis recréé (reconstruction complète, sans risque).
3. Construction des tables avec Spark (`model.py`).
4. Écriture de chaque table dans `gold_staging` (JDBC).
5. **Une seule transaction** : vidage des tables `gold`, recopie depuis `gold_staging`, contrôles
   (nombre de lignes, offres Gold = offres Silver, clés étrangères), écriture dans `gold.etl_runs`.
   En cas d'erreur : annulation complète, l'entrepôt reste dans son état précédent.
6. Rapport `_reports/gold_report.json` dans le bucket `job-offers-silver`.

## 3. Tables

Grain de la table de faits : **une ligne par offre unique**.

| Table | Clé | Contenu |
|---|---|---|
| `fact_job_offer` | `offer_key` (empreinte de `offer_id`, stable d'un chargement à l'autre) | clés des dimensions ; `title`, `url` ; `salary_annual_min/max/avg` ; `experience_years_min` ; `skills_count` ; `snapshots_seen` ; `days_online` ; `is_active`, `is_merged`, `contract_inferred` |
| `bridge_offer_skill` | (`offer_key`, `skill_key`) | une ligne par compétence citée dans une offre |
| `dim_date` | `date_key` (AAAAMMJJ) | jour, jour de la semaine (nom en français), semaine, mois (nom), trimestre, année, `year_month`, week-end |
| `dim_source` | `source_key` | code, nom affiché, méthode d'accès (API / Scraping) |
| `dim_contract` | `contract_key` | type de contrat × temps de travail, avec libellés |
| `dim_job_family` | `job_family_key` | famille de métier, libellé, `in_scope` (faux pour HORS_PERIMETRE) |
| `dim_experience` | `experience_key` | JUNIOR / CONFIRME / SENIOR / INCONNU, libellé, ordre de tri |
| `dim_education` | `education_key` | BAC+2 … DOCTORAT / INCONNU, libellé, ordre de tri |
| `dim_location` | `location_key` | ville, département, région, pays, latitude, longitude (médiane des coordonnées connues de la ville) |
| `dim_company` | `company_key` | nom de l'entreprise (« Non renseignée » si absent) |
| `dim_skill` | `skill_key` | 92 compétences du catalogue spaCy et leur catégorie |
| `etl_runs` | `run_id` | date de chargement, lignes Silver, faits, liaisons |

La table de faits a trois liens vers `dim_date` : publication (`date_posted_key`),
première observation (`first_seen_key`) et dernière observation (`last_seen_key`).

## 4. Calculs faits dans la couche Gold

- `salary_annual_avg` = moyenne de la fourchette, ou la seule borne connue.
- `skills_count` = nombre de compétences de l'offre.
- `days_online` = `last_seen − first_seen + 1`.
- Calendrier, libellés lisibles, indicateur de périmètre, valeurs manquantes remplacées par « Non renseignée ».

## 5. Vues

Les vues ne comptent que les offres du périmètre (`in_scope`).

| Vue | Colonnes |
|---|---|
| `gold_top_skills` | métier, compétence, catégorie, nb_offres, pourcentage_apparition, rang |
| `gold_repartition_geographique` | région, ville, nb_offres_actives |
| `gold_statistiques_salaires` | métier, type_contrat, expérience, nb_offres, salaire_min_moyen, salaire_max_moyen, salaire_median |
| `gold_tendances_recrutement` | mois_publication, métier, nb_offres_publiees |
| `v_offers` | une ligne par offre avec tous les libellés (exploration) |

## 6. Vérifier après un chargement

```bash
docker exec -it pfe_postgres psql -U <POSTGRES_USER> -d job_offers_dwh -c "SELECT * FROM gold.etl_runs ORDER BY run_id DESC LIMIT 3;"
docker exec -it pfe_postgres psql -U <POSTGRES_USER> -d job_offers_dwh -c "SELECT * FROM gold.gold_top_skills WHERE rang <= 5 ORDER BY metier, rang;"
```

Repères (données du 22/10/2026) : 3 080 offres, 9 469 liens offre-compétence, 116 offres hors périmètre.

## 7. Faire évoluer le schéma

Ajouter une colonne ou une table : modifier `schema.py` (DDL) et `model.py`, puis **incrémenter
`SCHEMA_VERSION`**. Au prochain lancement, le schéma est recréé automatiquement.
Ajouter une vue : l'ajouter dans `VIEWS` (`CREATE OR REPLACE VIEW`), elle sera créée au prochain lancement.

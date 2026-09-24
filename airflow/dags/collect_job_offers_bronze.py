"""
DAG : collecte quotidienne des offres d'emploi liées à la data
(France Travail + Adzuna, filtrées sur le mot-clé "data") et chargement
brut dans la couche Bronze (MinIO).

Schéma du DAG :

    extract_france_travail  ─┐
                              ├─▶ validate ─▶ load_to_bronze
    extract_adzuna          ─┘

Chaque étape d'extraction pousse sa liste d'offres en XCom ; l'étape de
validation vérifie juste qu'on a bien reçu des données exploitables avant
d'écrire dans MinIO (aucune transformation métier ici, volontairement --
c'est le rôle de la couche Silver en aval).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

default_args = {
    "owner": "pfe-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Mot-clé métier du projet : on ne collecte que les offres liées à la data.
# Filtrage fait ici, au niveau de la Collecte (paramètres transmis à l'appel
# API/scraping), jamais en Ingestion -- voir docs/data_dictionary.md pour le
# raisonnement complet sur où filtrer dans le pipeline.
JOB_KEYWORD = "data"


@dag(
    dag_id="collect_job_offers",
    description="Collecte quotidienne des offres d'emploi 'data' vers la couche Bronze (MinIO)",
    schedule="0 6 * * *",  # tous les jours à 06h00
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["pfe", "collecte", "bronze"],
)
def collect_job_offers():

    @task
    def extract_france_travail() -> list[dict]:
        from sources import france_travail

        # France Travail : le mot-clé se transmet via le paramètre "motsCles"
        offers = france_travail.collect(max_offers=1500, motsCles=JOB_KEYWORD)
        logger.info("France Travail : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
        return offers

    @task
    def extract_adzuna() -> list[dict]:
        from sources import adzuna

        # Adzuna : le mot-clé se transmet via le paramètre "what"
        offers = adzuna.collect(max_offers=1500, what=JOB_KEYWORD)
        logger.info("Adzuna : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
        return offers

    @task
    def validate(source_name: str, offers: list[dict]) -> list[dict]:
        if not offers:
            raise AirflowSkipException(f"Aucune offre reçue pour {source_name}, chargement ignoré.")
        logger.info("Validation OK pour %s : %d offres.", source_name, len(offers))
        return offers

    @task
    def load_to_bronze(source_name: str, offers: list[dict]) -> str:
        import os

        from storage.minio_client import write_json

        bucket = os.environ["MINIO_BUCKET_BRONZE"]
        object_key = write_json(bucket, source_name, offers)
        return object_key

    # France Travail
    ft_offers = extract_france_travail()
    ft_validated = validate.override(task_id="validate_france_travail")("france_travail", ft_offers)
    load_to_bronze.override(task_id="load_france_travail_to_bronze")("france_travail", ft_validated)

    # Adzuna
    adzuna_offers = extract_adzuna()
    adzuna_validated = validate.override(task_id="validate_adzuna")("adzuna", adzuna_offers)
    load_to_bronze.override(task_id="load_adzuna_to_bronze")("adzuna", adzuna_validated)


collect_job_offers()

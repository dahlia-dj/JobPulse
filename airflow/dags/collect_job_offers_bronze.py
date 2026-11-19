# """
# DAG : collecte quotidienne des offres d'emploi liées à la data
# (France Travail + Adzuna, filtrées sur le mot-clé "data") et chargement
# brut dans la couche Bronze (MinIO).

# Schéma du DAG :

#     extract_france_travail  ─┐
#                               ├─▶ validate ─▶ load_to_bronze
#     extract_adzuna          ─┘

# Chaque étape d'extraction pousse sa liste d'offres en XCom ; l'étape de
# validation vérifie juste qu'on a bien reçu des données exploitables avant
# d'écrire dans MinIO (aucune transformation métier ici, volontairement --
# c'est le rôle de la couche Silver en aval).
# """
# from __future__ import annotations

# import logging
# from datetime import datetime, timedelta

# from airflow.decorators import dag, task
# from airflow.exceptions import AirflowSkipException

# logger = logging.getLogger(__name__)

# default_args = {
#     "owner": "pfe-data-engineering",
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }

# # Mot-clé métier du projet : on ne collecte que les offres liées à la data.
# # Filtrage fait ici, au niveau de la Collecte (paramètres transmis à l'appel
# # API/scraping), jamais en Ingestion -- voir docs/data_dictionary.md pour le
# # raisonnement complet sur où filtrer dans le pipeline.
# JOB_KEYWORD = "data"


# @dag(
#     dag_id="collect_job_offers",
#     description="Collecte quotidienne des offres d'emploi 'data' vers la couche Bronze (MinIO)",
#     schedule="0 6 * * *",  # tous les jours à 06h00
#     start_date=datetime(2026, 9, 1),
#     catchup=False,
#     default_args=default_args,
#     tags=["pfe", "collecte", "bronze"],
# )
# def collect_job_offers():

#     @task
#     def extract_france_travail() -> list[dict]:
#         from sources import france_travail

#         # France Travail : le mot-clé se transmet via le paramètre "motsCles"
#         offers = france_travail.collect(max_offers=1500, motsCles=JOB_KEYWORD)
#         logger.info("France Travail : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
#         return offers

#     @task
#     def extract_adzuna() -> list[dict]:
#         from sources import adzuna

#         # Adzuna : le mot-clé se transmet via le paramètre "what"
#         offers = adzuna.collect(max_offers=1500, what=JOB_KEYWORD)
#         logger.info("Adzuna : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
#         return offers

#     @task
#     def validate(source_name: str, offers: list[dict]) -> list[dict]:
#         if not offers:
#             raise AirflowSkipException(f"Aucune offre reçue pour {source_name}, chargement ignoré.")
#         logger.info("Validation OK pour %s : %d offres.", source_name, len(offers))
#         return offers

#     @task
#     def load_to_bronze(source_name: str, offers: list[dict]) -> str:
#         import os

#         from storage.minio_client import write_json

#         bucket = os.environ["MINIO_BUCKET_BRONZE"]
#         object_key = write_json(bucket, source_name, offers)
#         return object_key

#     # France Travail
#     ft_offers = extract_france_travail()
#     ft_validated = validate.override(task_id="validate_france_travail")("france_travail", ft_offers)
#     load_to_bronze.override(task_id="load_france_travail_to_bronze")("france_travail", ft_validated)

#     # Adzuna
#     adzuna_offers = extract_adzuna()
#     adzuna_validated = validate.override(task_id="validate_adzuna")("adzuna", adzuna_offers)
#     load_to_bronze.override(task_id="load_adzuna_to_bronze")("adzuna", adzuna_validated)


# collect_job_offers()




"""
DAG : collecte quotidienne des offres d'emploi liées à la data, depuis 4
sources (2 par API : France Travail, Adzuna ; 2 par scraping : HelloWork,
Free-Work), et chargement brut dans la couche Bronze (MinIO).

Schéma du DAG (un seul DAG, 4 branches parallèles indépendantes) :

    extract_france_travail  ─▶ validate ─▶ load_to_bronze ─┐
    extract_adzuna           ─▶ validate ─▶ load_to_bronze ─┤
    extract_hellowork        ─▶ validate ─▶ load_to_bronze ─┼─▶ collection_done
    extract_freework         ─▶ validate ─▶ load_to_bronze ─┘

collection_done publie le Dataset BRONZE_DATASET : Airflow déclenche alors
automatiquement le DAG silver_job_offers (chaînage Collecte -> Silver -> Gold).

Chaque étape d'extraction pousse sa liste d'offres en XCom ; l'étape de
validation vérifie juste qu'on a bien reçu des données exploitables avant
d'écrire dans MinIO (aucune transformation métier ici, volontairement --
c'est le rôle de la couche Silver en aval).

Sources écartées (voir docs/data_dictionary.md) :
  - Welcome to the Jungle : son robots.txt interdit explicitement le chemin
    de recherche (`*/jobs?query=*`, `/*?`) à tous les robots ; le serveur
    renvoie la page d'accueil au lieu des résultats. Le connecteur
    sources/wttj.py est conservé dans le dépôt à titre de référence mais
    n'est plus branché ici.
  - APEC : protégée par DataDome (HTTP 403 dès le sitemap, même avec un
    simple GET), donc inaccessible sans contournement de la protection.
  - Indeed : HTTP 403 (Cloudflare) dès la page de liste, et son robots.txt
    interdit les pages détail (/viewjob) et la pagination profonde.

Pourquoi un seul DAG plutôt que deux (API vs scraping) : sous LocalExecutor,
séparer en plusieurs fichiers DAG n'isole pas réellement les dépendances ou
les pannes (tout tourne dans le même environnement Python) ; un seul DAG
avec des branches parallèles est plus simple à maintenir et tout aussi
robuste pour ce projet.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.datasets import Dataset
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

# Dataset « Bronze mis à jour » : même URI que dans bronze_to_silver.py.
BRONZE_DATASET = Dataset("s3://job-offers-bronze/snapshots")


@dag(
    dag_id="collect_job_offers",
    description="Collecte quotidienne des offres d'emploi data vers la couche Bronze (MinIO)",
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
        offers = france_travail.collect(max_offers=1000, motsCles=JOB_KEYWORD)
        logger.info("France Travail : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
        return offers

    @task
    def extract_adzuna() -> list[dict]:
        from sources import adzuna

        # Adzuna : le mot-clé se transmet via le paramètre "what".
        # Le connecteur trie par date (les plus récentes d'abord). 2 000 offres
        # (40 appels de 50) couvrent environ deux jours de publications : une offre
        # encore en ligne reste dans la fenêtre d'une collecte à l'autre et n'est
        # pas marquée à tort comme « disparue » (is_active) en couche Silver.
        # Compromis : au-delà, Adzuna (descriptions tronquées) pèserait trop
        # lourd face aux trois autres sources.
        offers = adzuna.collect(max_offers=2000, what=JOB_KEYWORD)
        logger.info("Adzuna : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
        return offers

    @task
    def extract_hellowork() -> list[dict]:
        from sources import hellowork

        offers = hellowork.collect(max_offers=500, keyword=JOB_KEYWORD)
        logger.info("HelloWork : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
        return offers

    @task
    def extract_freework() -> list[dict]:
        from sources import freework

        offers = freework.collect(max_offers=500, keyword=JOB_KEYWORD)
        logger.info("Free-Work : %d offres extraites (mot-clé: %s).", len(offers), JOB_KEYWORD)
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

    # Fin de collecte : une seule tâche, exécutée quand les 4 branches sont
    # terminées (réussies, ignorées OU en échec : trigger_rule="all_done"), pour
    # qu'une source en panne n'empêche pas de traiter les 3 autres.
    # outlets : sa réussite publie BRONZE_DATASET, qui déclenche le DAG Silver
    # une seule fois par collecte.
    @task(outlets=[BRONZE_DATASET], trigger_rule="all_done")
    def collection_done() -> None:
        logger.info("Collecte terminée : Bronze mis à jour, déclenchement de la couche Silver.")

    # France Travail
    ft_offers = extract_france_travail()
    ft_validated = validate.override(task_id="validate_france_travail")("france_travail", ft_offers)
    ft_loaded = load_to_bronze.override(task_id="load_france_travail_to_bronze")("france_travail", ft_validated)

    # Adzuna
    adzuna_offers = extract_adzuna()
    adzuna_validated = validate.override(task_id="validate_adzuna")("adzuna", adzuna_offers)
    adzuna_loaded = load_to_bronze.override(task_id="load_adzuna_to_bronze")("adzuna", adzuna_validated)

    # HelloWork
    hellowork_offers = extract_hellowork()
    hellowork_validated = validate.override(task_id="validate_hellowork")("hellowork", hellowork_offers)
    hellowork_loaded = load_to_bronze.override(task_id="load_hellowork_to_bronze")("hellowork", hellowork_validated)

    # Free-Work
    freework_offers = extract_freework()
    freework_validated = validate.override(task_id="validate_freework")("freework", freework_offers)
    freework_loaded = load_to_bronze.override(task_id="load_freework_to_bronze")("freework", freework_validated)

    [ft_loaded, adzuna_loaded, hellowork_loaded, freework_loaded] >> collection_done()


collect_job_offers()
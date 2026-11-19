"""
DAG : transformation Bronze -> Silver (nettoyage, normalisation, dédoublonnage, enrichissement).

Déclenchement : automatiquement à la fin de chaque collecte, quand le DAG
collect_job_offers publie le Dataset BRONZE_DATASET (chaînage Collecte -> Silver).
Il peut toujours être lancé à la main (bouton ▶) pour retraiter le Bronze.

En fin de DAG, la tâche check_quality publie le Dataset SILVER_DATASET : Airflow
déclenche alors automatiquement le DAG gold_job_offers (chaînage Silver -> Gold).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Même URI que dans collect_job_offers_bronze.py (publié en fin de collecte).
BRONZE_DATASET = Dataset("s3://job-offers-bronze/snapshots")
SILVER_DATASET = Dataset("s3://job-offers-silver/job_offers")


@dag(
    dag_id="silver_job_offers",
    description="Bronze (MinIO) -> Silver Parquet : nettoyage, normalisation, dédoublonnage, enrichissement",
    schedule=[BRONZE_DATASET],  # remplace l'ancien horaire fixe de 07:00
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "pfe-data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["pfe", "silver"],
)
def silver_job_offers():

    @task(execution_timeout=timedelta(minutes=30))
    def build_silver_layer() -> dict:
        from silver import job

        return job.run()

    # outlets : le Dataset n'est publié que si le contrôle qualité réussit,
    # donc la couche Gold n'est jamais chargée à partir d'un Silver douteux.
    @task(outlets=[SILVER_DATASET])
    def check_quality(report: dict) -> None:
        rows = report["silver_rows"]
        if rows < 100:
            raise ValueError(f"Silver anormalement petit : {rows} lignes")
        print(f"Silver OK : {rows} offres uniques, rapport : {report}")

    check_quality(build_silver_layer())


silver_job_offers()
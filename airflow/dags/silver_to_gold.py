"""
DAG : Silver (Parquet, MinIO) -> Gold (schéma en étoile, PostgreSQL job_offers_dwh).

Déclenché automatiquement par le Dataset SILVER_DATASET : dès que le DAG
silver_job_offers a écrit un nouveau Parquet et validé sa qualité, ce DAG démarre.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Même URI que dans silver_job_offers.py (c'est l'URI qui relie les deux DAG).
SILVER_DATASET = Dataset("s3://job-offers-silver/job_offers")


@dag(
    dag_id="gold_job_offers",
    description="Silver Parquet -> schéma en étoile PostgreSQL (couche Gold)",
    schedule=[SILVER_DATASET],
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "pfe-data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["pfe", "gold"],
)
def gold_job_offers():

    @task(execution_timeout=timedelta(minutes=30))
    def build_gold_layer() -> dict:
        from gold import job

        return job.run()

    @task
    def check_quality(report: dict) -> None:
        if report["fact_rows"] < 100:
            raise ValueError(f"Gold anormalement petit : {report['fact_rows']} offres")
        if report["fact_rows"] != report["silver_rows"]:
            raise ValueError(f"Gold ({report['fact_rows']}) et Silver ({report['silver_rows']}) ne concordent pas")
        print(f"Gold OK : {report}")

    check_quality(build_gold_layer())


gold_job_offers()

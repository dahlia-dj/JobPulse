"""
DAG : transformation Bronze -> Silver (nettoyage, normalisation, dédoublonnage).

Déclenchement manuel pour l'instant ; il sera chaîné à `collect_job_offers`
(Collecte -> Silver -> Gold) lors de l'étape d'orchestration finale.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="silver_job_offers",
    description="Bronze -> Silver Parquet : nettoyage, normalisation, dédoublonnage",
    schedule="0 7 * * *",
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

    @task
    def check_quality(report: dict) -> None:
        rows = report["silver_rows"]
        if rows < 100:
            raise ValueError(f"Silver anormalement petit : {rows} lignes")
        print(f"Silver OK : {rows} offres uniques, rapport : {report}")

    check_quality(build_silver_layer())


silver_job_offers()
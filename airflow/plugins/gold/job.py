"""
Job Gold : Silver (Parquet, MinIO) -> schéma en étoile (PostgreSQL, base job_offers_dwh).

Étapes :
  1. télécharge le Parquet Silver depuis MinIO ;
  2. construit dimensions, faits et table de liaison avec PySpark (gold.model) ;
  3. écrit chaque table dans le schéma de transit `gold_staging` (JDBC) ;
  4. dans UNE transaction PostgreSQL : vide les tables `gold`, les recharge depuis
     `gold_staging`, contrôle les comptages, puis valide. En cas d'erreur, rien
     n'est modifié : Power BI ne voit jamais un entrepôt à moitié chargé ;
  5. écrit un rapport qualité JSON dans MinIO.

Mode « reconstruction complète », comme la couche Silver : résultat déterministe et rejouable.
"""
from __future__ import annotations

import io
import json
import logging
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from gold.schema import DDL, LOAD_ORDER, SCHEMA_VERSION, VIEWS

logger = logging.getLogger(__name__)

SILVER_PREFIX = "job_offers"
REPORT_KEY = "_reports/gold_report.json"
JDBC_JAR = os.environ.get("GOLD_JDBC_JAR") or "/opt/spark-jars/postgresql-42.7.4.jar"


def _pg_params() -> dict:
    return {
        "host": os.environ.get("POSTGRES_DWH_HOST") or "postgres",
        "port": int(os.environ.get("POSTGRES_DWH_PORT") or 5432),
        "dbname": os.environ["POSTGRES_DWH_DB"],
        "user": os.environ["POSTGRES_DWH_USER"],
        "password": os.environ.get("POSTGRES_DWH_PASSWORD", ""),
    }


def _minio():
    from minio import Minio

    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def get_spark():
    """Session Spark locale avec le pilote JDBC PostgreSQL."""
    plugins_dir = str(Path(__file__).resolve().parents[1])
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (plugins_dir, os.environ.get("PYTHONPATH")) if p)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if not Path(JDBC_JAR).exists():
        raise FileNotFoundError(f"Pilote JDBC introuvable : {JDBC_JAR} (voir le Dockerfile)")

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("gold_job_offers")
        .config("spark.jars", JDBC_JAR)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def download_silver(client, bucket: str, dest: Path) -> int:
    n = 0
    for obj in client.list_objects(bucket, prefix=SILVER_PREFIX + "/", recursive=True):
        if obj.object_name.endswith(".parquet"):
            target = dest / Path(obj.object_name).name
            client.fget_object(bucket, obj.object_name, str(target))
            n += 1
    return n


def ensure_schema(conn) -> bool:
    """Crée le schéma `gold` s'il est absent ou d'une autre version (par exemple
    le squelette initial de postgres/init/02_create_gold_schema.sh). Renvoie True
    si le schéma a été (re)créé."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold_staging")
        cur.execute("SELECT to_regclass('gold.schema_version') IS NOT NULL")
        version = None
        if cur.fetchone()[0]:
            cur.execute("SELECT max(version) FROM gold.schema_version")
            version = cur.fetchone()[0]
        if version == SCHEMA_VERSION:
            cur.execute(VIEWS)  # CREATE OR REPLACE : les vues restent à jour
            conn.commit()
            return False
        logger.warning("Schéma gold absent ou version %s : recréation (version %s)", version, SCHEMA_VERSION)
        cur.execute("DROP SCHEMA IF EXISTS gold CASCADE")
        cur.execute("CREATE SCHEMA gold")
        cur.execute(DDL)
        cur.execute(VIEWS)
        cur.execute("INSERT INTO gold.schema_version VALUES (%s)", (SCHEMA_VERSION,))
    conn.commit()
    return True


def write_staging(tables: dict, params: dict) -> dict[str, int]:
    url = f"jdbc:postgresql://{params['host']}:{params['port']}/{params['dbname']}"
    counts = {}
    for name in LOAD_ORDER:
        df = tables[name].cache()
        counts[name] = df.count()
        (df.write.format("jdbc").mode("overwrite")
           .option("url", url).option("dbtable", f"gold_staging.{name}")
           .option("user", params["user"]).option("password", params["password"])
           .option("driver", "org.postgresql.Driver").option("batchsize", 5000)
           .save())
        logger.info("gold_staging.%s : %d lignes", name, counts[name])
    return counts


def swap_into_gold(conn, counts: dict[str, int], silver_rows: int) -> dict:
    """Recharge les tables gold depuis gold_staging dans une seule transaction."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE " + ", ".join(f"gold.{t}" for t in reversed(LOAD_ORDER)))
        for t in LOAD_ORDER:
            cur.execute(
                "SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position) "
                "FROM information_schema.columns WHERE table_schema = 'gold_staging' AND table_name = %s", (t,))
            cols = cur.fetchone()[0]
            cur.execute(f"INSERT INTO gold.{t} ({cols}) SELECT {cols} FROM gold_staging.{t}")
            if cur.rowcount != counts[t]:
                raise RuntimeError(f"gold.{t} : {cur.rowcount} lignes insérées, {counts[t]} attendues")

        checks = {}
        cur.execute("SELECT count(*) FROM gold.fact_job_offer")
        checks["fact_rows"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM gold.bridge_offer_skill")
        checks["bridge_rows"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM gold.fact_job_offer WHERE salary_annual_avg IS NOT NULL")
        checks["offers_with_salary"] = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM gold.fact_job_offer f JOIN gold.dim_job_family j USING (job_family_key) "
                    "WHERE NOT j.in_scope")
        checks["offers_out_of_scope"] = cur.fetchone()[0]
        if checks["fact_rows"] != silver_rows:
            raise RuntimeError(f"Faits : {checks['fact_rows']} lignes pour {silver_rows} offres Silver")

        cur.execute("INSERT INTO gold.etl_runs (silver_rows, fact_rows, bridge_rows) VALUES (%s, %s, %s)",
                    (silver_rows, checks["fact_rows"], checks["bridge_rows"]))
    conn.commit()  # les clés étrangères ont été vérifiées à chaque INSERT
    with conn.cursor() as cur:
        for t in LOAD_ORDER:
            cur.execute(f"DROP TABLE IF EXISTS gold_staging.{t}")
    conn.commit()
    return checks


def run() -> dict:
    from gold.model import build_star_schema

    silver_bucket = os.environ["MINIO_BUCKET_SILVER"]
    client = _minio()
    work = Path(tempfile.mkdtemp(prefix="gold_"))
    params = _pg_params()
    conn = psycopg2.connect(**params)
    try:
        n_files = download_silver(client, silver_bucket, work)
        if n_files == 0:
            raise RuntimeError(f"Aucun fichier Parquet sous {silver_bucket}/{SILVER_PREFIX}/")

        spark = get_spark()
        silver = spark.read.parquet(str(work)).cache()
        silver_rows = silver.count()
        if silver_rows == 0:
            raise RuntimeError("Silver vide : l'entrepôt n'est pas modifié")

        created = ensure_schema(conn)
        tables = build_star_schema(spark, silver)
        counts = write_staging(tables, params)
        checks = swap_into_gold(conn, counts, silver_rows)

        report = {
            "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "silver_rows": silver_rows,
            "schema_created": created,
            "tables": counts,
            **checks,
        }
        payload = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        client.put_object(silver_bucket, REPORT_KEY, io.BytesIO(payload), len(payload),
                          content_type="application/json")
        logger.info("Gold chargé : %s", report)
        return report
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        shutil.rmtree(work, ignore_errors=True)

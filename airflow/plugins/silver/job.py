"""
Job Silver : Bronze -> nettoyage / dédoublonnage (PySpark) -> Parquet (MinIO).

Mode « reconstruction complète » (annexe G du CDC) : à chaque exécution, tous
les snapshots Bronze sont relus et le dossier Silver est entièrement remplacé.
Le résultat est donc déterministe et rejouable (idempotent).
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

from minio import Minio

logger = logging.getLogger(__name__)

SILVER_PREFIX = "job_offers"


def _client() -> Minio:
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def get_spark():
    """Session Spark locale. Les workers Python (UDF) doivent retrouver le
    package `silver` : on fixe donc PYTHONPATH et PYSPARK_PYTHON."""
    plugins_dir = str(Path(__file__).resolve().parents[1])
    os.environ["PYTHONPATH"] = os.pathsep.join(
        p for p in (plugins_dir, os.environ.get("PYTHONPATH")) if p
    )
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("silver_job_offers")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def download_bronze(client: Minio, bucket: str, dest: Path) -> int:
    n = 0
    for obj in client.list_objects(bucket, recursive=True):
        if obj.is_dir or not obj.object_name.endswith(".json"):
            continue
        target = dest / obj.object_name
        target.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object(bucket, obj.object_name, str(target))
        n += 1
    return n


def upload_dir(client: Minio, bucket: str, local_dir: Path, prefix: str) -> int:
    """Remplace tout le préfixe `prefix` par le contenu de local_dir."""
    old = [o.object_name for o in client.list_objects(bucket, prefix=prefix + "/", recursive=True)]
    for name in old:
        client.remove_object(bucket, name)
    n = 0
    for f in sorted(local_dir.rglob("*")):
        if f.is_file() and not f.name.startswith(".") and not f.name.endswith(".crc"):
            client.fput_object(bucket, f"{prefix}/{f.relative_to(local_dir).as_posix()}", str(f))
            n += 1
    return n


def run() -> dict:
    from silver.transform import build_silver

    bronze_bucket = os.environ["MINIO_BUCKET_BRONZE"]
    silver_bucket = os.environ["MINIO_BUCKET_SILVER"]
    client = _client()
    if not client.bucket_exists(silver_bucket):
        client.make_bucket(silver_bucket)

    work = Path(tempfile.mkdtemp(prefix="silver_"))
    try:
        bronze_dir = work / "bronze"
        n_files = download_bronze(client, bronze_bucket, bronze_dir)
        if n_files == 0:
            raise RuntimeError(f"Aucun fichier Bronze dans {bronze_bucket}")
        logger.info("%d snapshots Bronze téléchargés", n_files)

        spark = get_spark()
        df, report = build_silver(spark, str(bronze_dir))
        df = df.cache()
        n_rows = df.count()
        if n_rows == 0:
            raise RuntimeError("Silver vide : on n'écrase pas la sortie précédente")

        out_dir = work / "out"
        df.coalesce(1).write.mode("overwrite").parquet(str(out_dir))
        n_up = upload_dir(client, silver_bucket, out_dir, SILVER_PREFIX)

        report.update(
            {
                "bronze_files": n_files,
                "silver_rows": n_rows,
                "parquet_files": n_up,
                "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
        payload = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        client.put_object(
            silver_bucket, "_reports/quality_report.json",
            io.BytesIO(payload), len(payload), content_type="application/json",
        )
        logger.info("Silver écrit : %d offres -> %s/%s", n_rows, silver_bucket, SILVER_PREFIX)
        return report
    finally:
        shutil.rmtree(work, ignore_errors=True)

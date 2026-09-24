"""
Client MinIO partagé pour l'écriture des données brutes en couche Bronze.

Convention de nommage des objets :
    {bucket}/{source}/date={AAAA-MM-JJ}/{source}_{AAAAMMJJ}_{HHMMSS}.json

Cette partition par date permet à Airflow de rejouer une exécution précise
sans toucher aux autres partitions (idempotence).
"""
from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime

from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def _get_client() -> Minio:
    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)


def ensure_bucket(bucket_name: str) -> None:
    client = _get_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Bucket MinIO créé : %s", bucket_name)


def write_json(bucket_name: str, source: str, records: list[dict], run_ts: datetime | None = None) -> str:
    """Écrit une liste d'enregistrements JSON bruts dans la couche Bronze.

    Retourne le chemin (object key) écrit dans MinIO, utile pour le XCom Airflow.
    """
    run_ts = run_ts or datetime.utcnow()
    client = _get_client()
    ensure_bucket(bucket_name)

    date_partition = run_ts.strftime("%Y-%m-%d")
    filename = f"{source}_{run_ts.strftime('%Y%m%d_%H%M%S')}.json"
    object_key = f"{source}/date={date_partition}/{filename}"

    payload = json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        client.put_object(
            bucket_name,
            object_key,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
    except S3Error as exc:
        logger.error("Échec de l'écriture MinIO pour %s : %s", object_key, exc)
        raise

    logger.info("%d enregistrements écrits dans %s/%s", len(records), bucket_name, object_key)
    return object_key


def read_json(bucket_name: str, object_key: str) -> list[dict]:
    client = _get_client()
    response = client.get_object(bucket_name, object_key)
    try:
        return json.loads(response.read())
    finally:
        response.close()
        response.release_conn()

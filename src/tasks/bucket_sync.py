import mimetypes
import os
import shutil
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config

from src.config import (
    LOG_LEVEL,
    RESOURCE_CACHE_DIR,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    SIRENE_DATA_DIR,
    check_s3_config,
)
from src.tasks.utils import get_logger

# Clés racine dans le bucket, à côté des clés existantes (decp/, siret_latlong.parquet)
RESOURCE_CACHE_PREFIX = "resource_cache"
SIRENE_PREFIX = "sirene"

SIRENE_COMPLETE_MARKER = "_COMPLETE"


def _s3_client():
    """Même configuration que src/tasks/get.py::get_from_s3 / publish.py::publish_to_s3."""
    if check_s3_config():
        raise ValueError(
            "Variables d'environnement S3 non définies : "
            + ", ".join(check_s3_config())
        )
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=30,
            read_timeout=300,
        ),
    )


def _list_objects(client, prefix: str) -> dict[str, dict]:
    """Clés du bucket sous prefix -> objet S3 (Size, LastModified...)."""
    objects = {}
    kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
    while True:
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            objects[obj["Key"]] = obj
        if not response.get("IsTruncated"):
            return objects
        kwargs["ContinuationToken"] = response["NextContinuationToken"]


def _list_keys(client, prefix: str) -> dict[str, int]:
    """Clés du bucket sous prefix -> taille (octets)."""
    return {key: obj["Size"] for key, obj in _list_objects(client, prefix).items()}


def _upload_file(client, local_path: Path, key: str):
    content_type, _ = mimetypes.guess_type(local_path.name)
    extra_args = {"ContentType": content_type} if content_type else {}
    client.upload_file(str(local_path), S3_BUCKET, key, ExtraArgs=extra_args)


def _download_file(
    client, key: str, local_path: Path, last_modified: datetime | None = None
):
    """Télécharge une clé du bucket vers un chemin local.

    Si last_modified est fourni, il devient l'atime/mtime du fichier : sans cela,
    remove_unused_cache (basé sur l'atime) ne ferait jamais expirer un fichier
    téléchargé sur une instance neuve.
    """
    local_path.parent.mkdir(exist_ok=True, parents=True)
    client.download_file(S3_BUCKET, key, str(local_path))
    if last_modified is not None:
        timestamp = last_modified.timestamp()
        os.utime(local_path, (timestamp, timestamp))


def upload_resource_cache():
    """Miroir local -> bucket du cache de ressources (parquet par ressource)."""
    logger = get_logger(level=LOG_LEVEL)
    client = _s3_client()

    local_files = {p.name: p for p in RESOURCE_CACHE_DIR.glob("*.parquet")}
    remote_keys = _list_keys(client, f"{RESOURCE_CACHE_PREFIX}/")

    for name, path in local_files.items():
        key = f"{RESOURCE_CACHE_PREFIX}/{name}"
        if remote_keys.get(key) != path.stat().st_size:
            logger.info(f"Upload {key}...")
            _upload_file(client, path, key)

    for key in remote_keys:
        if key.removeprefix(f"{RESOURCE_CACHE_PREFIX}/") not in local_files:
            logger.info(f"Suppression distante de {key} (absent du cache local)...")
            client.delete_object(Bucket=S3_BUCKET, Key=key)


def download_resource_cache():
    """Télécharge le cache de ressources du bucket, en sautant les fichiers déjà présents."""
    logger = get_logger(level=LOG_LEVEL)
    client = _s3_client()
    for key, obj in _list_objects(client, f"{RESOURCE_CACHE_PREFIX}/").items():
        local_path = RESOURCE_CACHE_DIR / key.removeprefix(f"{RESOURCE_CACHE_PREFIX}/")
        if local_path.exists() and local_path.stat().st_size == obj["Size"]:
            continue
        logger.info(f"Téléchargement de {key}...")
        _download_file(client, key, local_path, last_modified=obj["LastModified"])


def upload_sirene_data():
    """Miroir local -> bucket du dossier SIRENE du mois (produit par sirene_preprocess).

    Le marqueur _COMPLETE est retiré avant toute modification distante et
    réécrit en dernier : un upload interrompu laisse un dossier distant que
    download_sirene_data ignore.
    """
    logger = get_logger(level=LOG_LEVEL)
    client = _s3_client()
    prefix = f"{SIRENE_PREFIX}/{SIRENE_DATA_DIR.name}"
    marker_key = f"{prefix}/{SIRENE_COMPLETE_MARKER}"

    # Les temp_enrich_*.parquet sont écrits dans ce dossier par enrich_from_sirene
    local_files = {
        p.name: p
        for p in SIRENE_DATA_DIR.glob("*.parquet")
        if not p.name.startswith("temp_")
    }
    if not local_files:
        logger.warning(f"{SIRENE_DATA_DIR} vide ou inexistant : rien à uploader.")
        return

    remote_keys = _list_keys(client, f"{prefix}/")
    remote_keys.pop(marker_key, None)
    to_upload = [
        (f"{prefix}/{name}", path)
        for name, path in local_files.items()
        if remote_keys.get(f"{prefix}/{name}") != path.stat().st_size
    ]
    to_delete = [
        key for key in remote_keys if key.removeprefix(f"{prefix}/") not in local_files
    ]

    if to_upload or to_delete:
        client.delete_object(Bucket=S3_BUCKET, Key=marker_key)
    for key, path in to_upload:
        logger.info(f"Upload {key}...")
        _upload_file(client, path, key)
    for key in to_delete:
        logger.info(f"Suppression distante de {key}...")
        client.delete_object(Bucket=S3_BUCKET, Key=key)
    client.put_object(Bucket=S3_BUCKET, Key=marker_key, Body=b"")


def download_sirene_data():
    """Télécharge le dossier SIRENE du mois depuis le bucket, s'il n'existe pas localement.

    Même sémantique que decp_processing : un dossier présent est un dossier
    complet. D'où le téléchargement dans un dossier _partial renommé à la fin.
    """
    logger = get_logger(level=LOG_LEVEL)
    if SIRENE_DATA_DIR.exists():
        logger.info(f"{SIRENE_DATA_DIR} déjà présent localement.")
        return
    client = _s3_client()
    prefix = f"{SIRENE_PREFIX}/{SIRENE_DATA_DIR.name}"
    keys = _list_keys(client, f"{prefix}/")
    marker_key = f"{prefix}/{SIRENE_COMPLETE_MARKER}"
    if marker_key not in keys:
        logger.info(
            f"Pas de dossier SIRENE complet dans le bucket sous {prefix}. "
            "decp_processing exécutera sirene_preprocess() en ligne."
        )
        return

    partial_dir = SIRENE_DATA_DIR.with_name(f"{SIRENE_DATA_DIR.name}_partial")
    if partial_dir.exists():
        shutil.rmtree(partial_dir)
    for key in keys:
        if key == marker_key:
            continue
        logger.info(f"Téléchargement de {key}...")
        _download_file(client, key, partial_dir / key.removeprefix(f"{prefix}/"))
    partial_dir.rename(SIRENE_DATA_DIR)

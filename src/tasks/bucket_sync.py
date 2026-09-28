import mimetypes
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


def _list_keys(client, prefix: str) -> dict[str, int]:
    """Clés du bucket sous prefix -> taille (octets)."""
    keys = {}
    kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
    while True:
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            keys[obj["Key"]] = obj["Size"]
        if not response.get("IsTruncated"):
            return keys
        kwargs["ContinuationToken"] = response["NextContinuationToken"]


def _upload_file(client, local_path: Path, key: str):
    content_type, _ = mimetypes.guess_type(local_path.name)
    extra_args = {"ContentType": content_type} if content_type else {}
    client.upload_file(str(local_path), S3_BUCKET, key, ExtraArgs=extra_args)


def _download_file(client, key: str, local_path: Path):
    """Télécharge une clé du bucket vers un chemin local. L'appelant a déjà
    vérifié la pertinence du téléchargement (fichier absent localement)."""
    local_path.parent.mkdir(exist_ok=True, parents=True)
    client.download_file(S3_BUCKET, key, str(local_path))


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
    for key, size in _list_keys(client, f"{RESOURCE_CACHE_PREFIX}/").items():
        local_path = RESOURCE_CACHE_DIR / key.removeprefix(f"{RESOURCE_CACHE_PREFIX}/")
        if local_path.exists() and local_path.stat().st_size == size:
            continue
        logger.info(f"Téléchargement de {key}...")
        _download_file(client, key, local_path)


def upload_sirene_data():
    """Miroir local -> bucket du dossier SIRENE du mois (produit par sirene_preprocess)."""
    logger = get_logger(level=LOG_LEVEL)
    client = _s3_client()
    prefix = f"{SIRENE_PREFIX}/{SIRENE_DATA_DIR.name}"

    local_files = {p.name: p for p in SIRENE_DATA_DIR.glob("*.parquet")}
    if not local_files:
        logger.warning(f"{SIRENE_DATA_DIR} vide ou inexistant : rien à uploader.")
        return

    remote_keys = _list_keys(client, f"{prefix}/")
    for name, path in local_files.items():
        key = f"{prefix}/{name}"
        if remote_keys.get(key) != path.stat().st_size:
            logger.info(f"Upload {key}...")
            _upload_file(client, path, key)
    for key in remote_keys:
        if key.removeprefix(f"{prefix}/") not in local_files:
            logger.info(f"Suppression distante de {key}...")
            client.delete_object(Bucket=S3_BUCKET, Key=key)


def download_sirene_data():
    """Télécharge le dossier SIRENE du mois depuis le bucket, s'il n'existe pas déjà localement."""
    logger = get_logger(level=LOG_LEVEL)
    if SIRENE_DATA_DIR.exists() and any(SIRENE_DATA_DIR.glob("*.parquet")):
        logger.info(f"{SIRENE_DATA_DIR} déjà présent localement.")
        return
    client = _s3_client()
    prefix = f"{SIRENE_PREFIX}/{SIRENE_DATA_DIR.name}"
    keys = _list_keys(client, f"{prefix}/")
    if not keys:
        logger.info(
            f"Pas de dossier SIRENE dans le bucket sous {prefix}. "
            "decp_processing exécutera sirene_preprocess() en ligne."
        )
        return
    for key in keys:
        logger.info(f"Téléchargement de {key}...")
        _download_file(client, key, SIRENE_DATA_DIR / key.removeprefix(f"{prefix}/"))

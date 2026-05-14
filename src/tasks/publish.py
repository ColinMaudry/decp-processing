import mimetypes
from pathlib import Path

import boto3
from botocore.config import Config
from httpx import get, post, put

from src.config import (
    DATAGOUVFR_API,
    DATAGOUVFR_API_KEY,
    DECP_PROCESSING_PUBLISH_TIMEOUT,
    DIST_DIR,
    LOG_LEVEL,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
)
from src.tasks.utils import get_logger


def update_resource(dataset_id, resource_id, file_path, api_key):
    url = f"{DATAGOUVFR_API}/datasets/{dataset_id}/resources/{resource_id}/upload/"
    headers = {"X-API-KEY": api_key}
    file = {"file": open(file_path, "rb")}
    response = post(
        url, files=file, headers=headers, timeout=DECP_PROCESSING_PUBLISH_TIMEOUT
    ).raise_for_status()
    return response.json()


def publish_to_datagouv():
    logger = get_logger(level=LOG_LEVEL)

    dataset_id = "608c055b35eb4e6ee20eb325"

    uploads = [
        {
            "file": str(DIST_DIR / "decp.parquet"),
            "resource_id": "11cea8e8-df3e-4ed1-932b-781e2635e432",
        },
        {
            "file": str(DIST_DIR / "decp.csv"),
            "resource_id": "22847056-61df-452d-837d-8b8ceadbfc52",
        },
        # https://github.com/ColinMaudry/decp-processing/issues/124
        # {
        #     "file": str(DIST_DIR / "decp.sqlite"),
        #     "resource_id": "43f54982-da60-4eb7-aaaf-ba935396209b",
        # },
        {
            "file": str(DIST_DIR / "schema.json"),
            "resource_id": "9a4144c0-ee44-4dec-bee5-bbef38191d9a",
        },
        {
            "file": str(DIST_DIR / "statistiques_sources.csv"),
            "resource_id": "8ded94de-3b80-4840-a5bb-7faad1c9c234",
        },
        {
            "file": str(DIST_DIR / "statistiques_doublons_sources.parquet"),
            "resource_id": "a545bf6c-8b24-46ed-b49f-a32bf02eaffa",
        },
        {
            "file": str(DIST_DIR / "statistiques_marches.json"),
            "resource_id": "0ccf4a75-f3aa-4b46-8b6a-18aeb63e36df",
        },
        {
            "file": str(DIST_DIR / "probabilites_naf_cpv.csv"),
            "resource_id": "b6a502cd-560b-4350-a146-e837692f4b66",
        },
    ]

    for upload in uploads:
        logger.info(f"Mise à jour de {upload['file']}...")
        result = update_resource(
            dataset_id, upload["resource_id"], upload["file"], DATAGOUVFR_API_KEY
        )
        if result["success"] is True:
            logger.info("OK")


def get_resource_id(dataset_id, filepath) -> str or None:
    response = get(
        f"{DATAGOUVFR_API}/datasets/{dataset_id}/",
        headers={"X-API-KEY": DATAGOUVFR_API_KEY},
    ).raise_for_status()
    resources = response.json()["resources"]
    description = ""
    for resource in resources:
        if resource["title"] == str(filepath).split("/")[-1]:
            return resource["id"], None
        if resource["type"] == "main":
            description = resource["description"]
    return None, description


def publish_new_resource(dataset_id, file_path, description):
    # Upload de la nouvelle ressource
    url = f"{DATAGOUVFR_API}/datasets/{dataset_id}/upload/"
    headers = {"X-API-KEY": DATAGOUVFR_API_KEY}
    response = post(
        url,
        files={
            "file": open(file_path, "rb"),
        },
        headers=headers,
        timeout=DECP_PROCESSING_PUBLISH_TIMEOUT,
    ).raise_for_status()
    new_resource_id = response.json()["id"]

    # Màj des métadonnées
    url = f"{DATAGOUVFR_API}/datasets/{dataset_id}/resources/{new_resource_id}/"
    response = put(
        url,
        json={"title": str(file_path).split("/")[-1], "description": description},
        headers=headers,
    ).raise_for_status()

    return response.json()


def publish_to_s3(file: Path, prefix: str = "") -> None:
    logger = get_logger(level=LOG_LEVEL)

    missing = [
        name
        for name, value in [
            ("S3_ENDPOINT_URL", S3_ENDPOINT_URL),
            ("S3_BUCKET", S3_BUCKET),
            ("S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID),
            ("S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY),
            ("S3_REGION", S3_REGION),
        ]
        if not value
    ]
    if missing:
        raise ValueError(
            f"Variables d'environnement S3 non définies : {', '.join(missing)}"
        )

    file = Path(file)
    key = f"{prefix.strip('/')}/{file.name}" if prefix else file.name

    content_type, _ = mimetypes.guess_type(file.name)
    extra_args = {"ContentType": content_type} if content_type else {}

    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    logger.info(f"Publication de {file.name} sur s3://{S3_BUCKET}/{key}...")
    client.upload_file(str(file), S3_BUCKET, key, ExtraArgs=extra_args)
    logger.info("OK")


def publish_scrap_to_datagouv(year: str, month: str, file_path, target):
    dataset_ids = {
        "aws": "68caf6b135f19236a4f37a32",
        "marches-securises.fr": "68ebb48dd708fdb2d7c15bff",
        "dume": "694ff7a98210456475f98aca",
        "klekoon": "6952899077f982c9a2373ede",
    }
    logger = get_logger(level=LOG_LEVEL)

    dataset_id = dataset_ids[target]
    resource_id, description = get_resource_id(dataset_id, file_path)
    if resource_id is None:
        logger.info(f"Publication des données {target} de {year}-{month}...")
        result = publish_new_resource(dataset_id, file_path, description)
        if result:
            logger.info("OK (nouvelle ressource)")
    else:
        logger.info(f"Mise à jour des données {target} de {year}-{month}...")
        result = update_resource(dataset_id, resource_id, file_path, DATAGOUVFR_API_KEY)
        if result["success"] is True:
            logger.info("OK (mise à jour)")

"""Fichiers par source sur data.gouv.fr (dataset b, privé) : lecture et publication."""

from pathlib import Path

import httpx
import polars as pl

from src import config
from src.config import SourceDeao
from src.tasks.publish import update_resource

API_DATAGOUV = "https://www.data.gouv.fr/api/1"


class FichierSourceIndisponible(RuntimeError):
    """Le fichier d'une source est absent, en erreur ou illisible sur data.gouv.fr."""


def http_get(url: str, headers: dict) -> httpx.Response:
    """Seul accès réseau en lecture (monkeypatché en test)."""
    return config.HTTP_CLIENT.get(url, headers=headers, follow_redirects=True)


def telecharger_fichier_source(source: SourceDeao, dest_dir: Path) -> Path:
    headers = {"X-API-KEY": config.DATAGOUVFR_API_KEY or ""}
    url = (
        f"{API_DATAGOUV}/datasets/{config.DEAO_CLOSED_DATASET_ID}"
        f"/resources/{source.resource_id}/"
    )
    reponse = http_get(url, headers)
    if reponse.status_code != 200:
        raise FichierSourceIndisponible(
            f"Source {source.code} : ressource {source.resource_id} "
            f"indisponible (HTTP {reponse.status_code})"
        )
    fichier = http_get(reponse.json()["url"], headers)
    if fichier.status_code != 200:
        raise FichierSourceIndisponible(
            f"Source {source.code} : téléchargement impossible "
            f"(HTTP {fichier.status_code})"
        )
    chemin = Path(dest_dir) / f"{source.code}.parquet"
    chemin.write_bytes(fichier.content)
    try:
        pl.read_parquet_schema(chemin)
    except Exception as e:
        raise FichierSourceIndisponible(
            f"Source {source.code} : fichier parquet illisible ({e})"
        ) from e
    return chemin


def publier_fichier(dataset_id: str, resource_id: str, path: Path) -> None:
    resultat = update_resource(dataset_id, resource_id, path, config.DATAGOUVFR_API_KEY)
    if resultat.get("success") is not True:
        raise RuntimeError(
            f"Échec de la publication de {Path(path).name} "
            f"sur data.gouv.fr (ressource {resource_id})"
        )


def publication_autorisee() -> bool:
    """Publication si DECP_PROCESSING_PUBLISH et aucun SOLO_DEAO_SCRAPS non vide."""
    solo = [s for s in config.SOLO_DEAO_SCRAPS if s.strip()]
    return bool(config.DECP_PROCESSING_PUBLISH) and not solo

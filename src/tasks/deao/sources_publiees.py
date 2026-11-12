"""Fichiers par source sur data.gouv.fr (dataset b, privé) : lecture et publication."""

from pathlib import Path
from urllib.parse import urlparse

import httpx
import polars as pl

from src import config
from src.config import SourceDeao
from src.tasks.publish import update_resource
from src.tasks.utils import get_logger

API_DATAGOUV = "https://www.data.gouv.fr/api/1"


class FichierSourceIndisponible(RuntimeError):
    """Le fichier d'une source est absent, en erreur ou illisible sur data.gouv.fr."""


DELAIS = httpx.Timeout(30, read=300)


def _est_hote_datagouv(url: str) -> bool:
    hote = (urlparse(url).hostname or "").lower()
    return hote == "data.gouv.fr" or hote.endswith(".data.gouv.fr")


def _entetes(url: str) -> dict:
    """La clé API n'est envoyée qu'à data.gouv.fr (jamais à un hôte de fichiers tiers)."""
    if _est_hote_datagouv(url):
        return {"X-API-KEY": config.DATAGOUVFR_API_KEY or ""}
    return {}


def http_get(url: str, headers: dict) -> httpx.Response:
    """Accès réseau en lecture des métadonnées (monkeypatché en test)."""
    return config.HTTP_CLIENT.get(
        url, headers=headers, follow_redirects=True, timeout=DELAIS
    )


def http_telecharger(url: str, headers: dict, dest: Path) -> int:
    """Télécharge `url` en streaming vers `dest` si HTTP 200 ; renvoie le statut
    (monkeypatché en test)."""
    with config.HTTP_CLIENT.stream(
        "GET", url, headers=headers, follow_redirects=True, timeout=DELAIS
    ) as reponse:
        if reponse.status_code != 200:
            return reponse.status_code
        with open(dest, "wb") as f:
            for morceau in reponse.iter_bytes():
                f.write(morceau)
        return reponse.status_code


def telecharger_fichier_source(source: SourceDeao, dest_dir: Path) -> Path:
    url = (
        f"{API_DATAGOUV}/datasets/{config.DEAO_CLOSED_DATASET_ID}"
        f"/resources/{source.resource_id}/"
    )
    reponse = http_get(url, _entetes(url))
    if reponse.status_code != 200:
        raise FichierSourceIndisponible(
            f"Source {source.code} : ressource {source.resource_id} "
            f"indisponible (HTTP {reponse.status_code})"
        )
    url_fichier = reponse.json()["url"]
    chemin = Path(dest_dir) / f"{source.code}.parquet"
    temporaire = Path(dest_dir) / f"{source.code}.parquet.tmp"
    try:
        statut = http_telecharger(url_fichier, _entetes(url_fichier), temporaire)
        if statut != 200:
            raise FichierSourceIndisponible(
                f"Source {source.code} : téléchargement impossible (HTTP {statut})"
            )
        try:
            pl.read_parquet_schema(temporaire)
        except Exception as e:
            raise FichierSourceIndisponible(
                f"Source {source.code} : fichier parquet illisible ({e})"
            ) from e
        temporaire.replace(chemin)
    finally:
        temporaire.unlink(missing_ok=True)
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


def ecrire_et_publier(
    code: str, lignes: pl.DataFrame, entree: SourceDeao, chemin: Path
) -> str | None:
    """Écrit le parquet de la source, puis le publie si la publication est autorisée
    et le minimum de lignes atteint. Renvoie None si tout va bien, sinon la cause
    (lisible) ; ne lève pas pour un échec de publication."""
    logger = get_logger(level=config.LOG_LEVEL)
    logger.info(f"Écriture de {lignes.height} lignes {code} dans {chemin}...")
    lignes.write_parquet(chemin, compression="zstd")
    if not publication_autorisee():
        return None
    if lignes.height < entree.min_rows:
        cause = f"sous le minimum ({lignes.height} < {entree.min_rows}), non publié"
        logger.error(f"{code} : {cause}")
        return cause
    try:
        publier_fichier(config.DEAO_CLOSED_DATASET_ID, entree.resource_id, chemin)
    except Exception as e:
        logger.error(f"{code} : publication en erreur : {e!r}")
        return f"publication en erreur : {e}"
    return None

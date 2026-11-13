"""Archivage public des DCE Atexo dans S3 (même bucket que le reste, objets en lecture publique)."""

import re
from pathlib import PurePosixPath
from urllib.parse import quote

from botocore.exceptions import ClientError

from src import config
from src.tasks.scrap.deao.atexo.client import DceTelecharge
from src.tasks.utils import s3_client

# Objet de contrôle déposé puis supprimé avant le premier téléchargement d'un passage.
_CLE_CONTROLE = ".controle-depot"


class ErreurDepotS3(RuntimeError):
    """Échec d'un accès S3 des DCE (identifiants, ACL, quota…) : contrairement à un
    téléchargement en échec, il touche toutes les consultations du passage."""


def cle_dce(code: str, identifiant: str, nom_fichier: str) -> str:
    suffixe = PurePosixPath(nom_fichier).suffix.lower()
    if not re.fullmatch(r"\.[a-z0-9]{1,8}", suffixe):
        suffixe = ".zip"
    return f"{config.DEAO_DCE_S3_PREFIX}/{code}/{identifiant}{suffixe}"


def normaliser_taille(texte: str | None) -> str | None:
    """Taille affichée sûre pour une métadonnée S3 (ASCII) : espaces unifiés,
    caractères non ASCII encodés en pourcentage. À appliquer aussi à la taille
    affichée par la plateforme avant de la comparer à `taille_archivee`."""
    if texte is None:
        return None
    texte = re.sub(r"\s+", " ", texte).strip()
    return quote(texte, safe=" ,.()/%-+")


def _content_disposition(nom_fichier: str) -> str:
    ascii_sur = "".join(
        c if c.isascii() and c.isprintable() and c not in '"\\' else "_"
        for c in nom_fichier
        if c not in '"\\' and c.isprintable()
    )
    return (
        f'attachment; filename="{ascii_sur}"; '
        f"filename*=UTF-8''{quote(nom_fichier, safe='')}"
    )


def _base_publique() -> str:
    """Base des URL publiques des DCE : `DEAO_DCE_URL_PUBLIQUE` si renseignée (stockage
    multi-locataire : `<endpoint>/<compte>:<bucket>`), sinon `<endpoint>/<bucket>`."""
    base = (
        config.DEAO_DCE_URL_PUBLIQUE
        or f"{config.S3_ENDPOINT_URL.rstrip('/')}/{config.S3_BUCKET}"
    )
    return f"{base.rstrip('/')}/"


def url_publique(cle: str) -> str:
    return f"{_base_publique()}{cle}"


def cle_depuis_url(url: str | None) -> str | None:
    """Inverse de `url_publique` : None si l'URL n'est pas celle de notre bucket."""
    base = _base_publique()
    if url and url.startswith(base):
        return url[len(base) :]
    return None


def taille_archivee(cle: str) -> str | None:
    """Taille affichée par la plateforme, mémorisée à l'archivage ; None si absent."""
    try:
        reponse = s3_client().head_object(Bucket=config.S3_BUCKET, Key=cle)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return None
        raise ErreurDepotS3(f"Lecture de {cle} impossible : {exc!r}") from exc
    except Exception as exc:
        raise ErreurDepotS3(f"Lecture de {cle} impossible : {exc!r}") from exc
    return reponse.get("Metadata", {}).get("taille-affichee")


def verifier_depot_s3(code: str) -> None:
    """Dépose (en lecture publique), relit puis supprime un petit objet sous le préfixe
    des DCE (clé propre à la plateforme `code`, les plateformes tournant en parallèle) ;
    lève ErreurDepotS3 si S3 refuse l'une de ces opérations."""
    cle = f"{config.DEAO_DCE_S3_PREFIX}/{_CLE_CONTROLE}-{code}"
    try:
        client = s3_client()
        client.put_object(
            Bucket=config.S3_BUCKET, Key=cle, Body=b"ok", ACL="public-read"
        )
        client.head_object(Bucket=config.S3_BUCKET, Key=cle)
        client.delete_object(Bucket=config.S3_BUCKET, Key=cle)
    except Exception as exc:
        raise ErreurDepotS3(f"Contrôle du dépôt S3 en échec : {exc!r}") from exc


def deposer_dce(dce: DceTelecharge, cle: str, taille_affichee: str | None) -> str:
    """Dépose le DCE en lecture publique, supprime le fichier local, renvoie l'URL."""
    try:
        s3_client().upload_file(
            Filename=str(dce.chemin),
            Bucket=config.S3_BUCKET,
            Key=cle,
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": dce.content_type,
                "ContentDisposition": _content_disposition(dce.nom_fichier),
                "Metadata": {
                    "taille-affichee": normaliser_taille(taille_affichee) or ""
                },
            },
        )
    except Exception as exc:
        raise ErreurDepotS3(f"Dépôt de {cle} impossible : {exc!r}") from exc
    finally:
        dce.chemin.unlink(missing_ok=True)
    return url_publique(cle)

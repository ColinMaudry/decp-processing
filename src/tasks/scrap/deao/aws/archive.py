"""Archivage S3 des pages d'avis AWS (privées) et des règlements de consultation (publics)."""

from datetime import datetime
from pathlib import Path

from botocore.exceptions import ClientError

from src import config
from src.tasks.scrap.deao.atexo.dce import ErreurDepotS3, url_publique
from src.tasks.utils import s3_client

_CODES_ABSENT = ("404", "NoSuchKey", "NotFound")


def cle_avis(idm: str) -> str:
    return f"{config.DEAO_AWS_S3_PREFIX}/{idm}.html"


def cle_rc(code: str, idm: str) -> str:
    return f"{config.DEAO_RC_S3_PREFIX}/{code}/{idm}.pdf"


def marque_date_limite(d: datetime | None) -> str:
    """Date limite à la minute (ISO), chaîne vide si inconnue."""
    return d.isoformat(timespec="minutes") if d else ""


def lire_avis_archive(idm: str, date_limite: str | None) -> str | None:
    """HTML archivé de l'avis, ou None s'il est absent ou, quand `date_limite` est
    fournie, si la date limite archivée diffère (avis modifié depuis)."""
    cle = cle_avis(idm)
    try:
        reponse = s3_client().get_object(Bucket=config.S3_BUCKET, Key=cle)
        if (
            date_limite is not None
            and reponse.get("Metadata", {}).get("date-limite") != date_limite
        ):
            return None
        return reponse["Body"].read().decode("utf-8")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in _CODES_ABSENT:
            return None
        raise ErreurDepotS3(f"Lecture de {cle} impossible : {exc!r}") from exc
    except Exception as exc:
        raise ErreurDepotS3(f"Lecture de {cle} impossible : {exc!r}") from exc


def deposer_avis(idm: str, html: str, date_limite: str) -> None:
    """Dépose la page d'avis (objet privé) avec sa date limite en métadonnée."""
    cle = cle_avis(idm)
    try:
        s3_client().put_object(
            Bucket=config.S3_BUCKET,
            Key=cle,
            Body=html.encode("utf-8"),
            ContentType="text/html; charset=utf-8",
            Metadata={"date-limite": date_limite},
        )
    except Exception as exc:
        raise ErreurDepotS3(f"Dépôt de {cle} impossible : {exc!r}") from exc


def deposer_rc(chemin: Path, cle: str) -> str:
    """Dépose le RC en lecture publique, supprime le fichier local, renvoie l'URL."""
    try:
        s3_client().upload_file(
            Filename=str(chemin),
            Bucket=config.S3_BUCKET,
            Key=cle,
            ExtraArgs={"ACL": "public-read", "ContentType": "application/pdf"},
        )
    except Exception as exc:
        raise ErreurDepotS3(f"Dépôt de {cle} impossible : {exc!r}") from exc
    finally:
        chemin.unlink(missing_ok=True)
    return url_publique(cle)

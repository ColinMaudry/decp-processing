"""Archivage public des DCE Atexo dans S3 (même bucket que le reste, objets en lecture publique)."""

from pathlib import PurePosixPath

from botocore.exceptions import ClientError

from src import config
from src.tasks.scrap.deao.atexo.client import DceTelecharge
from src.tasks.utils import s3_client


def cle_dce(code: str, identifiant: str, nom_fichier: str) -> str:
    suffixe = PurePosixPath(nom_fichier).suffix.lower() or ".zip"
    return f"{config.DEAO_DCE_S3_PREFIX}/{code}/{identifiant}{suffixe}"


def _base_publique() -> str:
    return f"{config.S3_ENDPOINT_URL.rstrip('/')}/{config.S3_BUCKET}/"


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
        raise
    return reponse.get("Metadata", {}).get("taille-affichee")


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
                "ContentDisposition": f'attachment; filename="{dce.nom_fichier}"',
                "Metadata": {"taille-affichee": taille_affichee or ""},
            },
        )
    finally:
        dce.chemin.unlink(missing_ok=True)
    return url_publique(cle)

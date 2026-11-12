"""Résolution des identifiants eForms (uuid) en numéros de publication JOUE.

Un avis rectificatif ne connaît que l'uuid eForms de l'avis qu'il modifie ;
`idAvis` est le numéro de publication TED. On remplit `avisPrecedent` par la table
interne `idEforms -> idAvis`, puis par un cache S3, puis par l'API TED.
"""

import io
from collections import Counter
from collections.abc import Callable

import httpx
import polars as pl
from botocore.exceptions import ClientError

from src.config import DEAO_DATA_DIR, DEAO_GEOCODE_S3_PREFIX, LOG_LEVEL, S3_BUCKET
from src.tasks.deao.sources.joue.fetch import numero_publication
from src.tasks.publish import publish_to_s3
from src.tasks.utils import get_logger, s3_client

CACHE_FILENAME = "joue_identifiants.parquet"
SOURCE = "joue"

IDENTIFIANTS_SCHEMA = {"idEforms": pl.String, "idAvis": pl.String}


def charger_cache_identifiants() -> pl.DataFrame:
    """Cache S3 ; vide seulement si la clé n'existe pas, toute autre erreur remonte."""
    key = f"{DEAO_GEOCODE_S3_PREFIX.strip('/')}/{CACHE_FILENAME}"
    try:
        obj = s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return pl.DataFrame(schema=IDENTIFIANTS_SCHEMA)
        raise
    return (
        pl.read_parquet(io.BytesIO(obj["Body"].read()))
        .select(list(IDENTIFIANTS_SCHEMA))
        .cast(IDENTIFIANTS_SCHEMA)
    )


def resoudre_par_api(ids: list[str]) -> dict[str, str]:
    """Interroge TED ; un id introuvable ou en erreur HTTP reste non résolu."""
    logger = get_logger(level=LOG_LEVEL)
    resolus: dict[str, str] = {}
    for id_eforms in ids:
        try:
            numero = numero_publication(id_eforms)
        except httpx.HTTPError as e:
            logger.warning(f"Résolution TED impossible pour {id_eforms} : {e}")
            continue
        if numero:
            resolus[id_eforms] = numero
    return resolus


def resoudre_avec_cache(ids: list[str]) -> dict[str, str]:
    """Cache d'abord, API pour le reste ; le cache est réécrit s'il a grandi."""
    cache = charger_cache_identifiants()
    connus = dict(zip(cache["idEforms"], cache["idAvis"], strict=True))
    resolus = {i: connus[i] for i in ids if i in connus}
    manquants = [i for i in dict.fromkeys(ids) if i not in connus]
    if not manquants:
        return resolus

    nouveaux = resoudre_par_api(manquants)
    if nouveaux:
        complet = pl.concat(
            [
                cache,
                pl.DataFrame(
                    {"idEforms": list(nouveaux), "idAvis": list(nouveaux.values())},
                    schema=IDENTIFIANTS_SCHEMA,
                ),
            ]
        ).sort("idEforms")
        path = DEAO_DATA_DIR / CACHE_FILENAME
        complet.write_parquet(path)
        publish_to_s3(path, prefix=DEAO_GEOCODE_S3_PREFIX)
    return {**resolus, **nouveaux}


def resoudre_avis_precedents(
    avis: pl.DataFrame,
    stats: Counter,
    resolver: Callable[[list[str]], dict[str, str]] = resoudre_avec_cache,
) -> pl.DataFrame:
    """Remplit `avisPrecedent` des lignes JOUE à partir de `idEformsPrecedent`."""
    est_joue = pl.col("sourceDataset") == SOURCE
    a_resoudre = est_joue & pl.col("idEformsPrecedent").is_not_null()

    interne = dict(
        avis.filter(est_joue & pl.col("idEforms").is_not_null())
        .select("idEforms", "idAvis")
        .unique("idEforms")
        .iter_rows()
    )
    precedents = (
        avis.filter(a_resoudre)["idEformsPrecedent"].unique(maintain_order=True)
    ).to_list()

    correspondance = {i: interne[i] for i in precedents if i in interne}
    stats["joue:identifiant_interne"] += len(correspondance)

    restants = [i for i in precedents if i not in correspondance]
    if restants:
        externes = resolver(restants)
        correspondance.update(externes)
        stats["joue:identifiant_resolu"] += len(externes)
        stats["joue:identifiant_non_resolu"] += len(
            [i for i in restants if i not in externes]
        )

    return avis.with_columns(
        pl.when(a_resoudre)
        .then(pl.col("idEformsPrecedent").replace_strict(correspondance, default=None))
        .otherwise(pl.col("avisPrecedent"))
        .alias("avisPrecedent")
    )

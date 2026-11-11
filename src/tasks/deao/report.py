"""Rapport de run du flow deao_processing : compteurs, répartitions, taux de remplissage."""

from collections import Counter

import polars as pl
from prefect.artifacts import create_table_artifact
from prefect.exceptions import MissingContextError

from src.config import LOG_LEVEL
from src.tasks.deao.schema import PUBLISHED_FIELDS
from src.tasks.utils import get_logger

COMPTEURS = (
    "lu",
    "retenu",
    "erreur_xml",
    "jour_en_erreur",
    "chaine_annulation_seule",
    "uid_doublon",
)
_PREFIXES_COMPTEURS = ("exclu:", "non_mappe:")


def taux_remplissage(df: pl.DataFrame, par: str = "formatSource") -> pl.DataFrame:
    """Taux de remplissage (0-1) de chaque colonne publiée, par valeur de `par`.
    Une liste vide compte comme non remplie."""
    publiees = [f.name for f in PUBLISHED_FIELDS]
    colonnes = [c for c in df.columns if c in publiees and c != par]
    if par not in df.columns:
        df = df.with_columns(pl.lit("tous").alias(par))
    schema = {"colonne": pl.String, par: pl.String, "taux": pl.Float64}
    if not colonnes:
        return pl.DataFrame(schema=schema)

    frames = []
    for c in colonnes:
        rempli = pl.col(c).is_not_null()
        if isinstance(df.schema[c], pl.List):
            rempli = rempli & (pl.col(c).list.len() > 0)
        frames.append(
            df.group_by(par)
            .agg(rempli.mean().alias("taux"))
            .with_columns(pl.lit(c).alias("colonne"))
            .select("colonne", pl.col(par).cast(pl.String), "taux")
        )
    return pl.concat(frames).sort("colonne", par)


def lignes_rapport(stats: Counter, df: pl.DataFrame) -> list[dict]:
    lignes: list[dict] = []
    for cle in sorted(stats):
        if cle in COMPTEURS or cle.startswith(_PREFIXES_COMPTEURS):
            lignes.append({"indicateur": cle, "valeur": int(stats[cle])})

    if "statut" in df.columns:
        for valeur, n in df["statut"].value_counts().sort("statut").iter_rows():
            lignes.append({"indicateur": f"statut:{valeur}", "valeur": int(n)})

    if "acheteur_id_source" in df.columns:
        for valeur, n in (
            df["acheteur_id_source"]
            .value_counts()
            .sort("acheteur_id_source")
            .iter_rows()
        ):
            lignes.append(
                {
                    "indicateur": f"acheteur_id_source:{valeur if valeur is not None else 'null'}",
                    "valeur": int(n),
                }
            )

    par = "formatSource"
    taux = taux_remplissage(df, par)
    for colonne, format_source, valeur in taux.iter_rows():
        lignes.append(
            {
                "indicateur": f"remplissage:{colonne}:{format_source}",
                "valeur": round(float(valeur), 3),
            }
        )
    return lignes


def publier_rapport(lignes: list[dict]) -> None:
    logger = get_logger(level=LOG_LEVEL)
    for ligne in lignes:
        logger.info(f"{ligne['indicateur']} : {ligne['valeur']}")
    try:
        create_table_artifact(
            key="deao-processing-rapport",
            table=lignes,
            description="Rapport du run deao_processing",
        )
    except MissingContextError:
        logger.warning("Hors contexte Prefect : artefact du rapport non créé.")

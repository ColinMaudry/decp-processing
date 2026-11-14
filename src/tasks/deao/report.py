"""Rapport de run des flows scrap_open et deao_processing : compteurs, répartitions, taux de remplissage."""

import statistics
from collections import Counter, defaultdict

import polars as pl
from prefect.artifacts import create_table_artifact
from prefect.exceptions import MissingContextError

from src import config
from src.config import LOG_LEVEL
from src.tasks.deao.schema import PUBLISHED_FIELDS
from src.tasks.utils import get_logger, s3_client

COMPTEURS = (
    "lu",
    "retenu",
    "erreur_xml",
    "jour_en_erreur",
    "chaine_annulation_seule",
    "uid_doublon",
    "geocodage_en_erreur",
    "cycle",
    "jour_traite",
    "jour_vide",
    "erreur_mapping",
    "sans_date_publication",
    "chaine_recouvrement",
    "chaine_conservee",
    "identifiant_interne",
    "identifiant_resolu",
    "identifiant_non_resolu",
)
SOURCES_RAPPORT = ("boamp", "joue")
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


def _cle_publiee(cle: str) -> bool:
    """Clé publiée, après retrait d'un éventuel préfixe de source (« joue: »)."""
    base = cle.split(":", 1)[1] if cle.split(":", 1)[0] in SOURCES_RAPPORT else cle
    return base in COMPTEURS or base.startswith(_PREFIXES_COMPTEURS)


def lignes_rapport(stats: Counter, df: pl.DataFrame) -> list[dict]:
    lignes: list[dict] = []
    for cle in sorted(stats):
        if _cle_publiee(cle):
            lignes.append({"indicateur": cle, "valeur": int(stats[cle])})

    if "statut" in df.columns:
        for valeur, n in df["statut"].value_counts().sort("statut").iter_rows():
            lignes.append({"indicateur": f"statut:{valeur}", "valeur": int(n)})

    if "sourceDataset" in df.columns:
        for valeur, n in (
            df["sourceDataset"].value_counts().sort("sourceDataset").iter_rows()
        ):
            lignes.append(
                {
                    "indicateur": f"sourceDataset:{valeur if valeur is not None else 'null'}",
                    "valeur": int(n),
                }
            )

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
    if "sourceDataset" in df.columns and par in df.columns:
        # « boamp/eforms », « boamp/legacy », « joue/eforms »
        df = df.with_columns(
            pl.format("{}/{}", pl.col("sourceDataset"), pl.col(par)).alias(par)
        )
    taux = taux_remplissage(df, par)
    for colonne, format_source, valeur in taux.iter_rows():
        lignes.append(
            {
                "indicateur": f"remplissage:{colonne}:{format_source if format_source is not None else 'null'}",
                "valeur": round(float(valeur), 3),
            }
        )
    return lignes


def lignes_rapport_sources(df: pl.DataFrame) -> list[dict]:
    """Rapport de deao_processing : lignes, statuts, acheteur_id_source et taux de
    remplissage par `sourceDataset`."""
    lignes: list[dict] = []

    def _val(v) -> str:
        return v if v is not None else "null"

    for src, n in df["sourceDataset"].value_counts().sort("sourceDataset").iter_rows():
        lignes.append({"indicateur": f"lignes:{_val(src)}", "valeur": int(n)})
    for colonne, prefixe in (
        ("statut", "statut"),
        ("acheteur_id_source", "acheteur_id_source"),
    ):
        if colonne not in df.columns:
            continue
        for src, valeur, n in (
            df.group_by("sourceDataset", colonne)
            .len()
            .sort("sourceDataset", colonne)
            .iter_rows()
        ):
            lignes.append(
                {
                    "indicateur": f"{prefixe}:{_val(src)}:{_val(valeur)}",
                    "valeur": int(n),
                }
            )
    for colonne, src, valeur in taux_remplissage(df, par="sourceDataset").iter_rows():
        lignes.append(
            {
                "indicateur": f"remplissage:{colonne}:{_val(src)}",
                "valeur": round(float(valeur), 3),
            }
        )
    return lignes


def _mo(octets: float) -> float:
    return round(octets / 1_000_000, 1)


def lignes_rapport_dce(client=None) -> list[dict]:
    """Taille des DCE archivés dans S3 (dernière version de chaque DCE), en Mo :
    nombre, total, max, moyenne et médiane, puis nombre et total par plateforme.

    S3 indisponible : une seule ligne `dce:rapport_indisponible`, sans lever."""
    prefixe = f"{config.DEAO_DCE_S3_PREFIX}/"
    try:
        client = client or s3_client()
        par_code: dict[str, list[int]] = defaultdict(list)
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=config.S3_BUCKET, Prefix=prefixe
        )
        for page in pages:
            for objet in page.get("Contents", []):
                chemin = objet["Key"][len(prefixe) :]
                code, _, nom = chemin.partition("/")
                # Objets de contrôle du dépôt (« dce/.controle-depot-… »)
                if not nom or nom.startswith("."):
                    continue
                par_code[code].append(int(objet["Size"]))
    except Exception as e:
        get_logger(level=LOG_LEVEL).warning(f"Rapport des DCE impossible : {e!r}")
        return [{"indicateur": "dce:rapport_indisponible", "valeur": 1}]

    tailles = [t for liste in par_code.values() for t in liste]
    lignes = [{"indicateur": "dce:nombre", "valeur": len(tailles)}]
    if tailles:
        lignes += [
            {"indicateur": "dce:taille_totale_mo", "valeur": _mo(sum(tailles))},
            {"indicateur": "dce:taille_max_mo", "valeur": _mo(max(tailles))},
            {
                "indicateur": "dce:taille_moyenne_mo",
                "valeur": _mo(statistics.mean(tailles)),
            },
            {
                "indicateur": "dce:taille_mediane_mo",
                "valeur": _mo(statistics.median(tailles)),
            },
        ]
    for code in sorted(par_code):
        lignes += [
            {"indicateur": f"dce:nombre:{code}", "valeur": len(par_code[code])},
            {
                "indicateur": f"dce:taille_totale_mo:{code}",
                "valeur": _mo(sum(par_code[code])),
            },
        ]
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

"""Consolidation des lignes avis-lot en une ligne par appel d'offres × lot.

Chaînage des avis (initial → rectificatifs → annulation), choix de la ligne descriptive,
colonnes de cycle de vie et statut. Indépendant du format et de la source.
"""

from collections import Counter
from dataclasses import dataclass, field
from datetime import date

import polars as pl

from src.tasks.deao.schema import PUBLISHED_FIELDS
from src.tasks.utils import get_logger

CHAIN_KEYS = ["sourceDataset", "idAvisInitial"]
LOT_KEYS = [*CHAIN_KEYS, "lotId"]

# Colonnes publiées calculées à l'étape d'enrichissement, absentes à ce stade.
_ENRICHMENT_PREFIXES = (
    "acheteur_id_source",
    "acheteur_categorie",
    "acheteur_commune_",
    "acheteur_departement_",
    "acheteur_region_",
    "acheteur_latitude",
    "acheteur_longitude",
)
# Colonnes internes conservées pour l'enrichissement et le rapport.
INTERNAL_COLUMNS = [
    "formatSource",
    "acheteur_adresse_voie",
    "acheteur_adresse_cp",
    "acheteur_adresse_ville",
    "acheteur_adresse_nuts",
    "acheteur_pays",
]
OUTPUT_COLUMNS = [
    f.name for f in PUBLISHED_FIELDS if not f.name.startswith(_ENRICHMENT_PREFIXES)
] + INTERNAL_COLUMNS

# Ordre « du plus ancien au plus récent » : date de publication (nulle en premier, donc
# battue par toute date), puis IDWEB.
_ORDER = ["datePublicationAvis", "idAvis"]


@dataclass
class Consolidation:
    df: pl.DataFrame
    stats: Counter = field(default_factory=Counter)


def resolve_roots(
    avis: pl.DataFrame, max_iter: int = 20, stats: Counter | None = None
) -> pl.DataFrame:
    """Racine (`idAvisInitial`) de chaque avis, par remontée itérative de `avisPrecedent`.

    Un `avisPrecedent` absent des données reste la racine (uid stable). Les avis encore
    instables après `max_iter` itérations sont pris dans un cycle : ils deviennent leur
    propre racine, sont comptés (`stats["cycle"]`) et logués.
    """
    links = avis.unique(["sourceDataset", "idAvis"], keep="first").select(
        "sourceDataset", "idAvis", "avisPrecedent"
    )
    parents = links.filter(pl.col("avisPrecedent").is_not_null()).select(
        "sourceDataset",
        pl.col("idAvis").alias("idAvisInitial"),
        pl.col("avisPrecedent").alias("_parent"),
    )
    roots = links.select(
        "sourceDataset",
        "idAvis",
        pl.coalesce("avisPrecedent", "idAvis").alias("idAvisInitial"),
    )
    unstable = pl.Series(dtype=pl.Boolean)
    for _ in range(max_iter):
        step = roots.join(parents, on=CHAIN_KEYS, how="left")
        unstable = step["_parent"].is_not_null()
        if not unstable.any():
            break
        roots = step.select(
            "sourceDataset",
            "idAvis",
            pl.coalesce("_parent", "idAvisInitial").alias("idAvisInitial"),
        )
    else:
        step = roots.join(parents, on=CHAIN_KEYS, how="left")
        unstable = step["_parent"].is_not_null()

    n_cycles = int(unstable.sum()) if len(unstable) else 0
    if n_cycles:
        cycliques = step.filter(unstable)["idAvis"].head(20).to_list()
        get_logger("WARNING").warning(
            f"{n_cycles} avis pris dans un cycle de avisPrecedent "
            f"(racine = eux-mêmes), ex. : {cycliques}"
        )
        roots = step.select(
            "sourceDataset",
            "idAvis",
            pl.when(unstable)
            .then(pl.col("idAvis"))
            .otherwise(pl.col("idAvisInitial"))
            .alias("idAvisInitial"),
        )
    if stats is not None:
        stats["cycle"] += n_cycles
    return roots


def statut_expr(run_date: date, delai_jours: int) -> pl.Expr:
    """Statut de l'appel d'offres à `run_date` (voir la spec, « Cycle de vie »)."""
    jours_depuis_publication = (
        pl.lit(run_date) - pl.col("datePublicationInitiale")
    ).dt.total_days()
    return (
        pl.when(pl.col("annule"))
        .then(pl.lit("annule"))
        .when(pl.col("preInformation"))
        .then(pl.lit("pre-information"))
        .when(pl.col("dateLimiteReponse").is_not_null())
        .then(
            pl.when(pl.col("dateLimiteReponse").dt.date() < run_date)
            .then(pl.lit("clos"))
            .otherwise(pl.lit("ouvert"))
        )
        .when(jours_depuis_publication < delai_jours)
        .then(pl.lit("ouvert"))
        .otherwise(pl.lit("clos"))
        .alias("statut")
    )


def _chain_aggregates(avis: pl.DataFrame) -> pl.DataFrame:
    """Colonnes de cycle de vie, une ligne par chaîne d'avis."""
    est_racine = pl.col("idAvis") == pl.col("idAvisInitial")
    return (
        avis.unique(["sourceDataset", "idAvis"], keep="first")
        .with_columns(est_racine.alias("_estRacine"))
        .sort([*CHAIN_KEYS, *_ORDER], nulls_last=False)
        .group_by(CHAIN_KEYS, maintain_order=True)
        .agg(
            pl.col("idAvis").unique().sort().alias("avisLies"),
            pl.col("idAvis").last().alias("idDernierAvis"),
            pl.col("urlAvis").last(),
            pl.col("datePublicationAvis").last().alias("_dateDernierAvis"),
            pl.coalesce(
                pl.col("datePublicationAvis").filter(pl.col("_estRacine")).first(),
                pl.col("datePublicationAvis").min(),
            ).alias("datePublicationInitiale"),
            pl.col("datePublicationAvis")
            .filter(pl.col("typeAvis") != "initial")
            .max()
            .alias("dateDerniereModification"),
            (pl.col("typeAvis") == "rectificatif").any().alias("rectifie"),
            (pl.col("typeAvis") == "annulation").any().alias("annule"),
            pl.col("infosRectificatif").drop_nulls().alias("infosRectificatifs"),
            pl.col("preInformation")
            .filter(pl.col("_estRacine"))
            .first()
            .alias("preInformation"),
            (pl.col("typeAvis") == "annulation").all().alias("_seulementAnnulations"),
        )
        .with_columns(pl.col("preInformation").fill_null(False))
    )


def _descriptive_rows(avis: pl.DataFrame) -> pl.DataFrame:
    """Ligne descriptive par (chaîne, lot) : celle du dernier avis complet contenant le lot,
    ou, pour une chaîne sans avis complet, celle du dernier rectificatif.

    Toutes les lignes du dernier avis sont gardées (un lot en double dans un même avis
    est traité ensuite comme un doublon d'uid).
    """
    complet = pl.col("avisComplet").fill_null(False)
    complets = avis.filter(complet)
    rectificatifs = avis.filter(~complet & (pl.col("typeAvis") == "rectificatif")).join(
        complets.select(CHAIN_KEYS).unique(), on=CHAIN_KEYS, how="anti"
    )
    return (
        pl.concat([complets, rectificatifs])
        .sort([*LOT_KEYS, *_ORDER], nulls_last=False)
        .filter(pl.col("idAvis") == pl.col("idAvis").last().over(LOT_KEYS))
    )


def consolidate(avis: pl.DataFrame, run_date: date, delai_jours: int) -> Consolidation:
    """Une ligne par appel d'offres × lot à partir des lignes avis-lot de toutes les sources."""
    logger = get_logger("INFO")
    stats: Counter = Counter()

    avis = avis.join(
        resolve_roots(avis, stats=stats), on=["sourceDataset", "idAvis"], how="left"
    )
    chains = _chain_aggregates(avis)

    seulement_annulations = chains.filter(pl.col("_seulementAnnulations"))
    stats["chaine_annulation_seule"] += seulement_annulations.height
    avis = avis.join(
        seulement_annulations.select(CHAIN_KEYS), on=CHAIN_KEYS, how="anti"
    )

    df = (
        _descriptive_rows(avis)
        .drop("urlAvis", "preInformation")
        .join(chains.drop("_seulementAnnulations"), on=CHAIN_KEYS, how="inner")
        .with_columns(
            pl.concat_str(LOT_KEYS, separator="_").alias("uid"),
            statut_expr(run_date, delai_jours),
        )
    )

    avant = df.height
    # Doublons d'uid = même chaîne et même lotId : le dernier avis est le même pour
    # toutes ces lignes, on départage donc par le contenu (plus petit lotIntitule,
    # puis objet, puis description) pour un résultat indépendant de l'ordre d'entrée.
    df = df.sort(
        ["uid", "lotIntitule", "objet", "description"],
        nulls_last=True,
        maintain_order=True,
    ).unique("uid", keep="first", maintain_order=True)
    stats["uid_doublon"] += avant - df.height
    if stats["uid_doublon"]:
        logger.warning(
            f"{stats['uid_doublon']} lignes en doublon d'uid supprimées "
            "(conservation de la ligne au plus petit lotIntitule/objet/description)"
        )

    df = df.select(OUTPUT_COLUMNS).sort("uid")
    logger.info(
        f"Consolidation : {avant} lignes appel d'offres × lot → {df.height} ; "
        f"suivi : {dict(stats)}"
    )
    return Consolidation(df=df, stats=stats)

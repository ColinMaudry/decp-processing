"""Dédoublonnage BOAMP ↔ plateformes (profils d'acheteur) dans deao_processing.

Une même consultation peut être annoncée au BOAMP et publiée sur une plateforme.
Les lignes BOAMP sont conservées et complétées par la plateforme ; la ligne
plateforme est retirée. En cas de doute, pas de fusion : mieux vaut afficher deux
fois une consultation que fusionner deux consultations distinctes.
"""

from collections import Counter
from urllib.parse import urlsplit

import polars as pl

from src.config import SourceDeao
from src.tasks.deao.enrich import normaliser_nom_expr

SOURCE_BOAMP = "boamp"
CLE = ["sourceDataset", "id"]
# Hôte d'une URL (sans identifiants ni port), en minuscules
_HOTE = r"^[A-Za-z][A-Za-z0-9+.\-]*://(?:[^/?#@]*@)?([^/?#:]+)"
_ID_CONSULTATION = r"(?:[?&]id=|/consultation/)(\d+)"
# Valeurs de la plateforme reportées sur les lignes BOAMP appariées : la première
# remplace celle du BOAMP, les autres ne complètent que les nuls.
_REMPLACEES = ("urlConsultation",)
_COMPLETEES = (
    "urlDce",
    "urlReglementConsultation",
    "acheteur_codePostal",
    "referenceAcheteur",
)
_NIVEAU_CONSULTATION = (
    "urlConsultation",
    "urlDce",
    "urlReglementConsultation",
    "acheteur_codePostal",
    "referenceAcheteur",
    "acheteur_id",
    "dateLimiteReponse",
)


def id_consultation_expr(url_base: str) -> pl.Expr:
    """Identifiant de consultation lu dans `urlConsultation` si l'URL est sur le
    site de `url_base` (hôte identique, casse ignorée), sinon nul."""
    hote = (urlsplit(url_base).hostname or "").lower()
    url = pl.col("urlConsultation")
    return (
        pl.when(url.str.extract(_HOTE, 1).str.to_lowercase() == hote)
        .then(url.str.extract(_ID_CONSULTATION, 1))
        .otherwise(None)
    )


def _consultations(df: pl.DataFrame) -> pl.DataFrame:
    """Une ligne par consultation (sourceDataset, id) : première valeur non nulle
    des champs de niveau consultation (identiques d'un lot à l'autre)."""
    return df.group_by(CLE, maintain_order=True).agg(
        pl.col(c).drop_nulls().first() for c in _NIVEAU_CONSULTATION
    )


def _cle_stricte(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute la clé du rapprochement strict ; lignes à clé incomplète écartées."""
    return (
        df.with_columns(
            normaliser_nom_expr("referenceAcheteur").alias("_ref"),
            pl.col("dateLimiteReponse").dt.date().alias("_jour"),
        )
        .filter(
            pl.col("_ref").is_not_null()
            & (pl.col("_ref") != "")
            & pl.col("acheteur_id").is_not_null()
            & pl.col("_jour").is_not_null()
        )
        .with_columns(pl.len().over("_ref", "acheteur_id", "_jour").alias("_n"))
    )


def dedoublonner_sources(
    df: pl.DataFrame, registre: tuple[SourceDeao, ...]
) -> tuple[pl.DataFrame, Counter]:
    """Fusionne les consultations publiées à la fois au BOAMP et sur une plateforme
    du registre (`open_data: false` avec `url_base`).

    Rapprochement exact par l'URL BOAMP (même hôte, même identifiant), puis strict
    parmi les consultations restantes : même référence normalisée, même SIRET non
    nul, même jour limite, et un seul candidat de chaque côté."""
    stats = Counter(dedoublonnage_retire=0)
    presentes = set(df["sourceDataset"].unique().drop_nulls())
    plateformes = [
        s for s in registre if not s.open_data and s.url_base and s.code in presentes
    ]
    if not plateformes:
        return df, stats

    consultations = _consultations(df)
    boamp = consultations.filter(pl.col("sourceDataset") == SOURCE_BOAMP)
    codes = [p.code for p in plateformes]
    cons_plateformes = consultations.filter(pl.col("sourceDataset").is_in(codes))

    # 1. Exact : l'URL BOAMP désigne une consultation de la plateforme
    boamp = boamp.with_columns(
        pl.coalesce(
            pl.when(id_consultation_expr(p.url_base).is_not_null()).then(
                pl.struct(
                    pl.lit(p.code).alias("source_p"),
                    id_consultation_expr(p.url_base).alias("id_p"),
                )
            )
            for p in plateformes
        ).alias("_cible")
    ).unnest("_cible")
    paires_exactes = boamp.select(
        "id", "source_p", "id_p", pl.lit("fusion_exacte").alias("regle")
    ).join(
        cons_plateformes.select(
            pl.col("sourceDataset").alias("source_p"), pl.col("id").alias("id_p")
        ),
        on=["source_p", "id_p"],
        how="inner",
    )

    # 2. Strict, parmi les consultations non appariées. Une consultation BOAMP dont
    # l'URL désigne une consultation d'une plateforme n'est pas candidate : son
    # URL dit déjà de quelle consultation il s'agit.
    boamp_restant = boamp.filter(pl.col("source_p").is_null())
    plateformes_restant = cons_plateformes.join(
        paires_exactes.select(
            pl.col("source_p").alias("sourceDataset"), pl.col("id_p").alias("id")
        ),
        on=CLE,
        how="anti",
    )
    cle = ["_ref", "acheteur_id", "_jour"]
    paires_strictes = (
        _cle_stricte(boamp_restant)
        .filter(pl.col("_n") == 1)
        .select("id", *cle)
        .join(
            _cle_stricte(plateformes_restant)
            .filter(pl.col("_n") == 1)
            .select(
                pl.col("sourceDataset").alias("source_p"),
                pl.col("id").alias("id_p"),
                *cle,
            ),
            on=cle,
            how="inner",
        )
        .select("id", "source_p", "id_p", pl.lit("fusion_stricte").alias("regle"))
    )

    paires = pl.concat([paires_exactes, paires_strictes])
    for code in codes:
        fusionnees = paires.filter(pl.col("source_p") == code)
        for regle in ("fusion_exacte", "fusion_stricte"):
            stats[f"{regle}:{code}"] = fusionnees.filter(pl.col("regle") == regle)[
                "id_p"
            ].n_unique()
        stats[f"seule:{code}"] = (
            cons_plateformes.filter(pl.col("sourceDataset") == code).height
            - fusionnees["id_p"].n_unique()
        )

    # 3. Fusion : lignes BOAMP complétées, lignes plateforme retirées
    valeurs = paires.join(
        cons_plateformes.select(
            pl.col("sourceDataset").alias("source_p"),
            pl.col("id").alias("id_p"),
            *(pl.col(c).alias(f"_p_{c}") for c in (*_REMPLACEES, *_COMPLETEES)),
        ),
        on=["source_p", "id_p"],
        how="left",
    ).select(
        pl.lit(SOURCE_BOAMP).alias("sourceDataset"),
        "id",
        *(f"_p_{c}" for c in (*_REMPLACEES, *_COMPLETEES)),
    )
    colonnes = df.columns
    avant = df.height
    df = (
        df.join(
            paires.select(
                pl.col("source_p").alias("sourceDataset"), pl.col("id_p").alias("id")
            ).unique(),
            on=CLE,
            how="anti",
            maintain_order="left",
        )
        .join(valeurs, on=CLE, how="left", maintain_order="left")
        .with_columns(
            *(pl.coalesce(f"_p_{c}", c).alias(c) for c in _REMPLACEES),
            *(pl.coalesce(c, f"_p_{c}").alias(c) for c in _COMPLETEES),
        )
        .select(colonnes)
    )
    stats["dedoublonnage_retire"] = avant - df.height
    return df, stats

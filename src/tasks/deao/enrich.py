from collections.abc import Callable

import polars as pl

from src.config import PAYS_FRANCAIS
from src.tasks.deao.geocode_cache import adresse_cle_expr
from src.tasks.deao.values import departement_depuis_cp, departement_depuis_nuts
from src.tasks.enrich import add_etablissement_data, add_unite_legale_data

COLONNES_GEO = [
    "acheteur_commune_code",
    "acheteur_commune_nom",
    "acheteur_departement_code",
    "acheteur_departement_nom",
    "acheteur_region_code",
    "acheteur_region_nom",
    "acheteur_latitude",
    "acheteur_longitude",
]
COLONNES_SIRENE = ["acheteur_categorie", *COLONNES_GEO]
_NOM_SIRENE = "_acheteur_nom_sirene"
_TROUVE = "_sirene_trouve"


def normaliser_nom_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .str.to_lowercase()
        .str.normalize("NFKD")
        .str.replace_all(r"\p{M}", "")
        .str.replace_all(r"[^a-z0-9]+", " ")
        .str.strip_chars()
    )


def appliquer_dictionnaire_siret(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        normaliser_nom_expr("acheteur_nom").alias("_nom_norm"),
    ).with_columns(
        pl.when(pl.col("_nom_norm") == "")
        .then(None)
        .otherwise(pl.col("_nom_norm"))
        .alias("_nom_norm")
    )
    cle = ["_nom_norm", "acheteur_adresse_cp"]
    dictionnaire = (
        df.filter(
            pl.col("acheteur_id").is_not_null()
            & pl.col("_nom_norm").is_not_null()
            & pl.col("acheteur_adresse_cp").is_not_null()
        )
        .group_by(cle)
        .agg(pl.col("acheteur_id").unique())
        .filter(pl.col("acheteur_id").list.len() == 1)
        .select(*cle, pl.col("acheteur_id").list.first().alias("_siret_dico"))
    )
    df = df.join(dictionnaire, on=cle, how="left", nulls_equal=False)
    return df.with_columns(
        pl.when(pl.col("acheteur_id").is_not_null())
        .then(pl.lit("avis"))
        .when(pl.col("_siret_dico").is_not_null())
        .then(pl.lit("dictionnaire"))
        .otherwise(None)
        .alias("acheteur_id_source"),
        pl.coalesce("acheteur_id", "_siret_dico").alias("acheteur_id"),
    ).drop("_nom_norm", "_siret_dico")


def enrichir_sirene(
    df: pl.DataFrame,
    lf_unites_legales: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
) -> pl.DataFrame:
    sirets = df.select("acheteur_id").drop_nulls().unique().lazy()
    sirets = add_unite_legale_data(
        sirets, lf_unites_legales, siret_column="acheteur_id", type_siret="acheteur"
    )
    sirets = add_etablissement_data(
        sirets, lf_etablissements, "acheteur_id", "acheteur"
    )
    sirene = sirets.select(
        "acheteur_id",
        pl.col("acheteur_nom").alias(_NOM_SIRENE),
        *COLONNES_SIRENE,
        pl.lit(True).alias(_TROUVE),
    ).collect()
    # Un SIRET présent plusieurs fois dans SIRENE dupliquerait les lignes au join.
    sirene = sirene.unique("acheteur_id", keep="first", maintain_order=True)
    sirene = sirene.with_columns(
        pl.col("acheteur_latitude").cast(pl.Float64),
        pl.col("acheteur_longitude").cast(pl.Float64),
    )
    df = df.drop([c for c in COLONNES_SIRENE if c in df.columns])
    return (
        df.join(sirene, on="acheteur_id", how="left")
        .with_columns(
            pl.coalesce(_NOM_SIRENE, "acheteur_nom").alias("acheteur_nom"),
            pl.col(_TROUVE).fill_null(False),
        )
        .drop(_NOM_SIRENE)
    )


def _remplir(existant: str, nouveau: str) -> pl.Expr:
    """Conserve la valeur existante ; ne comble que les nuls."""
    return pl.coalesce(existant, nouveau).alias(existant)


def enrichir_geographie(
    df: pl.DataFrame,
    df_cog: pl.DataFrame,
    geocoder: Callable[[pl.DataFrame], pl.DataFrame],
) -> pl.DataFrame:
    df = df.with_columns(adresse_cle_expr())
    adresses = (
        df.filter(~pl.col(_TROUVE) & pl.col("adresse_cle").is_not_null())
        .select(
            "adresse_cle",
            pl.concat_str(
                [
                    pl.col("acheteur_adresse_voie").fill_null(""),
                    pl.col("acheteur_adresse_ville").fill_null(""),
                ],
                separator=" ",
            )
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias("q"),
            pl.col("acheteur_adresse_cp").alias("postcode"),
        )
        .unique(subset="adresse_cle", keep="first", maintain_order=True)
    )

    if adresses.height > 0:
        geo = (
            geocoder(adresses)
            .filter(pl.col("status") == "success")
            .select("adresse_cle", "commune_code", "latitude", "longitude")
            .unique(subset="adresse_cle", keep="first")
        )
        communes = df_cog.select(
            "commune_code",
            pl.col("commune_nom").alias("_commune_nom"),
            pl.col("departement_code").alias("_departement_code"),
            pl.col("departement_nom").alias("_departement_nom"),
            pl.col("region_code").alias("_region_code"),
            pl.col("region_nom").alias("_region_nom"),
        ).unique(subset="commune_code", keep="first")
        geo = geo.join(communes, on="commune_code", how="left").rename(
            {
                "commune_code": "_commune_code",
                "latitude": "_latitude",
                "longitude": "_longitude",
            }
        )
        df = df.join(geo, on="adresse_cle", how="left")
        pas_sirene = ~pl.col(_TROUVE)
        df = df.with_columns(
            pl.when(pas_sirene)
            .then(_remplir(f"acheteur_{c}", f"_{c}"))
            .otherwise(pl.col(f"acheteur_{c}"))
            .alias(f"acheteur_{c}")
            for c in (
                "commune_code",
                "commune_nom",
                "departement_code",
                "departement_nom",
                "region_code",
                "region_nom",
                "latitude",
                "longitude",
            )
        ).drop(
            "_commune_code",
            "_commune_nom",
            "_departement_code",
            "_departement_nom",
            "_region_code",
            "_region_nom",
            "_latitude",
            "_longitude",
        )

    # Replis département : code postal puis NUTS3 (fonctions Python : valeurs uniques)
    cps = df.select("acheteur_adresse_cp").drop_nulls().unique()
    cps = cps.with_columns(
        pl.col("acheteur_adresse_cp")
        .map_elements(departement_depuis_cp, return_dtype=pl.String)
        .alias("_dep_cp")
    )
    nuts = df.select("acheteur_adresse_nuts").drop_nulls().unique()
    nuts = nuts.with_columns(
        pl.col("acheteur_adresse_nuts")
        .map_elements(departement_depuis_nuts, return_dtype=pl.String)
        .alias("_dep_nuts")
    )
    departements = df_cog.select(
        pl.col("departement_code").alias("_dep"),
        pl.col("departement_nom").alias("_dep_nom"),
        pl.col("region_code").alias("_dep_region_code"),
        pl.col("region_nom").alias("_dep_region_nom"),
    ).unique(subset="_dep", keep="first")
    df = (
        df.join(cps, on="acheteur_adresse_cp", how="left")
        .join(nuts, on="acheteur_adresse_nuts", how="left")
        .with_columns(
            pl.coalesce("acheteur_departement_code", "_dep_cp", "_dep_nuts").alias(
                "acheteur_departement_code"
            )
        )
        .join(
            departements,
            left_on="acheteur_departement_code",
            right_on="_dep",
            how="left",
        )
        .with_columns(
            _remplir("acheteur_departement_nom", "_dep_nom"),
            _remplir("acheteur_region_code", "_dep_region_code"),
            _remplir("acheteur_region_nom", "_dep_region_nom"),
        )
        .drop(
            "adresse_cle",
            "_dep_cp",
            "_dep_nuts",
            "_dep_nom",
            "_dep_region_code",
            "_dep_region_nom",
        )
    )
    return df


def neutraliser_acheteurs_etrangers(df: pl.DataFrame) -> pl.DataFrame:
    """Met à nul identifiant et adresse des acheteurs hors périmètre français.

    Leurs codes postaux ressemblent parfois à des codes français : ils ne doivent
    produire ni SIRET, ni département, ni requête de géocodage. Un pays nul
    (anciennes partitions) vaut France."""
    if "acheteur_pays" not in df.columns:
        return df
    etranger = pl.col("acheteur_pays").is_not_null() & ~pl.col("acheteur_pays").is_in(
        list(PAYS_FRANCAIS)
    )
    colonnes = [
        "acheteur_id",
        "acheteur_adresse_voie",
        "acheteur_adresse_cp",
        "acheteur_adresse_ville",
        "acheteur_adresse_nuts",
    ]
    return df.with_columns(
        pl.when(etranger).then(None).otherwise(pl.col(c)).alias(c) for c in colonnes
    )


def enrich_acheteurs(
    df: pl.DataFrame,
    lf_unites_legales: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
    df_cog: pl.DataFrame,
    geocoder: Callable[[pl.DataFrame], pl.DataFrame],
) -> pl.DataFrame:
    n = df.height
    df = neutraliser_acheteurs_etrangers(df)
    df = appliquer_dictionnaire_siret(df)
    df = enrichir_sirene(df, lf_unites_legales, lf_etablissements)
    df = enrichir_geographie(df, df_cog, geocoder)
    if df.height != n:
        raise RuntimeError(
            f"L'enrichissement acheteur a modifié le nombre de lignes ({n} → {df.height})"
        )
    return df.with_columns(acheteur_codePostal=pl.col("acheteur_adresse_cp")).drop(
        _TROUVE
    )

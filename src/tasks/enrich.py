from datetime import datetime

import polars as pl
import polars.selectors as cs

from src.config import ACHETEURS_NON_SIRENE, LOG_LEVEL, SIRENE_DATA_DIR
from src.tasks.transform import (
    extract_unique_acheteurs_siret,
    extract_unique_titulaires_siret,
)
from src.tasks.utils import get_logger


def add_etablissement_data(
    lf_sirets: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
    siret_column: str,
    type_siret: str,
) -> pl.LazyFrame:
    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données (how = inner)
    lf_sirets = lf_sirets.join(
        lf_etablissements, how="inner", left_on=siret_column, right_on="siret"
    )

    # On ne prend pas l'activité des acheteurs
    if type_siret == "acheteur":
        lf_sirets = lf_sirets.drop(cs.starts_with("activite_"))

    # Si il y a un etablissement_nom (Enseigne1Etablissement ou denominationUsuelleEtablissement),
    # on l'ajoute au nom de l'organisme, entre parenthèses
    lf_sirets = lf_sirets.with_columns(
        pl.when(
            pl.col("etablissement_nom").is_not_null()
            & (pl.col("etablissement_nom") != pl.col(f"{type_siret}_nom"))
        )
        .then(
            pl.concat_str(
                pl.col(f"{type_siret}_nom"),
                pl.lit(" ("),
                pl.col("etablissement_nom"),
                pl.lit(")"),
            )
        )
        .otherwise(pl.col(f"{type_siret}_nom"))
        .alias(f"{type_siret}_nom")
    ).drop("etablissement_nom")

    lf_sirets = lf_sirets.rename(
        {
            "latitude": f"{type_siret}_latitude",
            "longitude": f"{type_siret}_longitude",
            "commune_code": f"{type_siret}_commune_code",
            "commune_nom": f"{type_siret}_commune_nom",
            "departement_code": f"{type_siret}_departement_code",
            "departement_nom": f"{type_siret}_departement_nom",
            "region_code": f"{type_siret}_region_code",
            "region_nom": f"{type_siret}_region_nom",
        }
    )
    return lf_sirets


def add_unite_legale_data(
    lf_sirets: pl.LazyFrame,
    unites_legales_lf: pl.LazyFrame,
    siret_column: str,
    type_siret: str,
) -> pl.LazyFrame:
    # Extraction du SIREN à partir du SIRET (9 premiers caractères)
    lf_sirets = lf_sirets.with_columns(pl.col(siret_column).str.head(9).alias("siren"))

    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données
    lf_sirets = lf_sirets.join(unites_legales_lf, how="inner", on="siren")
    lf_sirets = lf_sirets.rename(
        {
            "denominationUniteLegale": f"{type_siret}_nom",
            "siren": f"{type_siret}_siren",
            "categorieEntreprise": f"{type_siret}_categorie",
        }
    )

    if type_siret == "acheteur":
        lf_sirets = lf_sirets.drop(f"{type_siret}_categorie")

    return lf_sirets


def enrich_from_sirene(lf: pl.LazyFrame):
    logger = get_logger(level=LOG_LEVEL)

    # Récupération des données SIRET/SIREN préparées dans sirene-preprocess()
    lf_etablissements = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")
    lf_unites_legales = pl.scan_parquet(SIRENE_DATA_DIR / "unites_legales.parquet")

    lf_base = lf.clone()

    # DONNÉES SIRENE ACHETEURS

    logger.info("Extraction des SIRET des acheteurs...")
    lf_sirets_acheteurs = extract_unique_acheteurs_siret(lf_base)

    logger.info("Ajout des données unités légales (acheteurs)...")
    lf_sirets_acheteurs = add_unite_legale_data(
        lf_sirets_acheteurs,
        lf_unites_legales,
        siret_column="acheteur_id",
        type_siret="acheteur",
    )

    logger.info("Ajout des données établissements (acheteurs)...")
    lf_sirets_acheteurs = add_etablissement_data(
        lf_sirets_acheteurs, lf_etablissements, "acheteur_id", "acheteur"
    )

    # Matérialisation de sirets_acheteurs pour rompre
    # le cercle de dépendances du lf
    acheteurs_path = SIRENE_DATA_DIR / "temp_enrich_acheteurs.parquet"
    lf_sirets_acheteurs.sink_parquet(acheteurs_path)
    lf_sirets_acheteurs = pl.scan_parquet(acheteurs_path)

    # DONNÉES SIRENE TITULAIRES

    logger.info("Extraction des SIRET des titulaires...")
    lf_sirets_titulaires = extract_unique_titulaires_siret(lf_base)

    logger.info("Ajout des données unités légales (titulaires)...")
    lf_sirets_titulaires = add_unite_legale_data(
        lf_sirets_titulaires,
        lf_unites_legales,
        siret_column="titulaire_id",
        type_siret="titulaire",
    )

    logger.info("Ajout des données établissements (titulaires)...")
    lf_sirets_titulaires = add_etablissement_data(
        lf_sirets_titulaires, lf_etablissements, "titulaire_id", "titulaire"
    )

    #  # Matérialisation de sirets_titulaires pour rompre
    # le cercle de dépendances du lf
    titulaires_path = SIRENE_DATA_DIR / "temp_enrich_titulaires.parquet"
    lf_sirets_titulaires.sink_parquet(titulaires_path)
    lf_sirets_titulaires = pl.scan_parquet(titulaires_path)

    # JOINTURES

    lf = lf.join(lf_sirets_acheteurs, how="left", on="acheteur_id")

    # Fallback pour les acheteurs absents de SIRENE (ex: Ministère des Armées)
    lf = apply_acheteurs_non_sirene_fallback(lf)

    # En joignant en utilisant à la fois le SIRET et le typeIdentifiant, on s'assure qu'on ne joint pas sur
    # des id de titulaires non-SIRET
    lf = lf.join(
        lf_sirets_titulaires,
        how="left",
        on=["titulaire_id", "titulaire_typeIdentifiant"],
    )

    del lf_sirets_titulaires
    # logger.info("Amélioration des données unités légales des titulaires...")
    # lf_sirets_titulaires = improve_titulaire_unite_legale_data(lf_sirets_titulaires)

    lf = calculate_distance(lf)

    lf = lf.drop(cs.ends_with("_siren"))

    return lf


def apply_acheteurs_non_sirene_fallback(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applique les données d'acheteurs non présents dans SIRENE en fallback.

    Les acheteurs absents de SIRENE ET du fallback conservent acheteur_nom = NULL.
    """
    if not ACHETEURS_NON_SIRENE:
        return lf

    # Créer un DataFrame de fallback à partir du dictionnaire
    fallback_data = [
        {"acheteur_id": siret, "acheteur_nom_fallback": data["nom"]}
        for siret, data in ACHETEURS_NON_SIRENE.items()
    ]
    lf_fallback = pl.LazyFrame(fallback_data)

    # Joindre avec les données de fallback
    lf = lf.join(lf_fallback, on="acheteur_id", how="left")

    # Remplacer acheteur_nom NULL par la valeur de fallback (si disponible)
    lf = lf.with_columns(
        pl.coalesce("acheteur_nom", "acheteur_nom_fallback").alias("acheteur_nom")
    ).drop("acheteur_nom_fallback")

    return lf


def calculate_distance(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Utilisation de polars_ds.haversine
    # https://polars-ds-extension.readthedocs.io/en/latest/num.html#polars_ds.exprs.num.haversine
    lf = lf.with_columns(
        haversine(
            pl.col("acheteur_latitude"),
            pl.col("acheteur_longitude"),
            pl.col("titulaire_latitude"),
            pl.col("titulaire_longitude"),
        )
        .round(mode="half_away_from_zero")
        .cast(pl.Int16)
        .alias("titulaire_distance")
    )
    return lf


def haversine(
    lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr, R: float = 6371.0
) -> pl.Expr:
    """
    Calcule la distance haversine entre deux points (lat1, lon1) et (lat2, lon2) en km.
    Utilise des opérations vectorisées Polars.
    Généré par la LLM Euria, développée et hébergée en Suisse par Infomaniak.
    """
    # Convertir en radians
    lat1 = pl.col("acheteur_latitude").radians()
    lon1 = pl.col("acheteur_longitude").radians()
    lat2 = pl.col("titulaire_latitude").radians()
    lon2 = pl.col("titulaire_longitude").radians()

    # Différences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Formule haversine
    a = (dlat / 2.0).sin().pow(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().pow(2)
    c = 2.0 * a.sqrt().arcsin()

    # Distance
    return R * c


def add_type_marche(lf: pl.LazyFrame) -> pl.LazyFrame:
    cpv_division = pl.col("codeCPV").str.slice(0, 2).cast(pl.Int8, strict=False)

    lf = lf.with_columns(
        pl.when(cpv_division == 45)
        .then(pl.lit("Travaux"))
        .when(cpv_division.is_in(range(1, 45)) | (cpv_division == 48))
        .then(pl.lit("Fournitures"))
        .when(cpv_division.is_in(range(50, 99)))
        .then(pl.lit("Services"))
        .otherwise(pl.lit("Non catégorisé"))
        .fill_null(pl.lit("Code CPV invalide"))
        .alias("type")
    )
    return lf


def add_duree_restante(lff: pl.LazyFrame):
    today = datetime.now().date()
    duree_mois_days_int = pl.col("dureeMois") * 30.5
    end_date = pl.col("dateNotification") + pl.duration(days=duree_mois_days_int)
    duree_restante_mois = ((end_date - today).dt.total_days() / 30).round(1)

    # Pas de valeurs négatives.
    lff = lff.with_columns(
        pl.when(duree_restante_mois < 0)
        .then(pl.lit(0))
        .otherwise(duree_restante_mois)
        .alias("dureeRestanteMois")
    )

    # Si dureeRestanteMois > dureeMois, dureeRestanteMois = dureeMois
    lff = lff.with_columns(
        pl.when(pl.col("dureeRestanteMois") > pl.col("dureeMois"))
        .then(pl.col("dureeMois").cast(pl.Float32))
        .otherwise(pl.col("dureeRestanteMois"))
        .alias("dureeRestanteMois")
    )
    return lff

import polars as pl
import polars.selectors as cs
from polars_ds import haversine
from prefect import task

from src.config import SIRENE_DATA_DIR
from src.tasks.transform import (
    extract_unique_acheteurs_siret,
    extract_unique_titulaires_siret,
)


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
        pl.when(pl.col("etablissement_nom").is_not_null())
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
        {"denominationUniteLegale": f"{type_siret}_nom", "siren": f"{type_siret}_siren"}
    )

    return lf_sirets


@task(log_prints=True)
def enrich_from_sirene(lf: pl.LazyFrame):
    # Récupération des données SIRET/SIREN préparées dans sirene-preprocess()
    lf_etablissements = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")
    lf_unites_legales = pl.scan_parquet(SIRENE_DATA_DIR / "unites_legales.parquet")

    lf_base = lf.clone()

    # DONNÉES SIRENE ACHETEURS

    print("Extraction des SIRET des acheteurs...")
    lf_sirets_acheteurs = extract_unique_acheteurs_siret(lf_base)

    print("Ajout des données établissements (acheteurs)...")
    lf_sirets_acheteurs = add_etablissement_data(
        lf_sirets_acheteurs, lf_etablissements, "acheteur_id", "acheteur"
    )

    print("Ajout des données unités légales (acheteurs)...")
    lf_sirets_acheteurs = add_unite_legale_data(
        lf_sirets_acheteurs,
        lf_unites_legales,
        siret_column="acheteur_id",
        type_siret="acheteur",
    )

    # Matérialisation de sirets_acheteurs pour rompre
    # le cercle de dépendances du lf
    acheteurs_path = SIRENE_DATA_DIR / "temp_enrich_acheteurs.parquet"
    lf_sirets_acheteurs.sink_parquet(acheteurs_path)
    lf_sirets_acheteurs = pl.scan_parquet(acheteurs_path)

    # DONNÉES SIRENE TITULAIRES

    print("Extraction des SIRET des titulaires...")
    lf_sirets_titulaires = extract_unique_titulaires_siret(lf_base)

    print("Ajout des données établissements (titulaires)...")
    lf_sirets_titulaires = add_etablissement_data(
        lf_sirets_titulaires, lf_etablissements, "titulaire_id", "titulaire"
    )

    print("Ajout des données unités légales (titulaires)...")
    lf_sirets_titulaires = add_unite_legale_data(
        lf_sirets_titulaires,
        lf_unites_legales,
        siret_column="titulaire_id",
        type_siret="titulaire",
    )

    #  # Matérialisation de sirets_titulaires pour rompre
    # le cercle de dépendances du lf
    titulaires_path = SIRENE_DATA_DIR / "temp_enrich_titulaires.parquet"
    lf_sirets_titulaires.sink_parquet(titulaires_path)
    lf_sirets_titulaires = pl.scan_parquet(titulaires_path)

    # JOINTURES

    lf = lf.join(lf_sirets_acheteurs, how="left", on="acheteur_id")

    # En joignant en utilisant à la fois le SIRET et le typeIdentifiant, on s'assure qu'on ne joint pas sur
    # des id de titulaires non-SIRET
    lf = lf.join(
        lf_sirets_titulaires,
        how="left",
        on=["titulaire_id", "titulaire_typeIdentifiant"],
    )

    del lf_sirets_titulaires
    # print("Amélioration des données unités légales des titulaires...")
    # lf_sirets_titulaires = improve_titulaire_unite_legale_data(lf_sirets_titulaires)

    lf = calculate_distance(lf)

    lf = lf.drop(cs.ends_with("_siren"))

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
        .alias("distance")
    )
    return lf

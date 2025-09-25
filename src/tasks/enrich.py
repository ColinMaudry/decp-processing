import polars as pl
import polars.selectors as cs
from prefect import task

from config import SIRENE_DATA_DIR
from tasks.transform import (
    extract_unique_acheteurs_siret,
    extract_unique_titulaires_siret,
)


def add_etablissement_data(
    df: pl.LazyFrame, etablissement_columns: list, siret_column: str
) -> pl.LazyFrame:
    # Récupération des données SIRET titulaires
    schema_etablissements = {
        "siret": "object",
        "siren": "object",
        "longitude": "float",
        "latitude": "float",
        "activitePrincipaleEtablissement": "object",
        "codeCommuneEtablissement": "object",
        "etatAdministratifEtablissement": "category",
    }
    # TODO: fix
    etablissement_df_chunked = pl.scan_csv(
        SIRENE_DATA_DIR / "etablissements.parquet",
        dtype=schema_etablissements,
        index_col=None,
        usecols=["siret"] + etablissement_columns,
    )

    df = pl.merge(
        df,
        etablissement_df_chunked,
        how="inner",
        left_on="titulaire_id",
        right_on="siret",
    )
    return df


def add_unite_legale_data(
    df: pl.LazyFrame, df_sirets: pl.LazyFrame, siret_column: str, type_siret: str
) -> pl.LazyFrame:
    # Extraction du SIREN à partir du SIRET (9 premiers caractères)
    df_sirets = df_sirets.with_columns(pl.col(siret_column).str.head(9).alias("siren"))

    # Récupération des données des unités légales issues du flow de preprocess
    unites_legales_lf = pl.scan_parquet(SIRENE_DATA_DIR / "unites_legales.parquet")

    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données
    df_sirets = df_sirets.join(unites_legales_lf, how="inner", on="siren")
    df_sirets = df_sirets.rename(
        {"denominationUniteLegale": f"{type_siret}_nom", "siren": f"{type_siret}_siren"}
    )
    if type_siret == "acheteur":
        # Ajout des données acheteurs enrichies au df de base
        df = df.join(df_sirets, how="left", on="acheteur_id")
    elif type_siret == "titulaire":
        # En joignant en utilisant à la fois le SIRET et le typeIdentifiant, on s'assure qu'on ne joint pas sur
        # des id de titulaires non-SIRET
        df = df.join(
            df_sirets, how="left", on=["titulaire_id", "titulaire_typeIdentifiant"]
        )
    return df


@task(log_prints=True)
def enrich_from_sirene(df: pl.LazyFrame):
    # DONNÉES SIRENE ACHETEURS

    print("Extraction des SIRET des acheteurs...")
    df_sirets_acheteurs = extract_unique_acheteurs_siret(df.clone())

    # print("Ajout des données établissements (acheteurs)...")
    # df_sirets_acheteurs = add_etablissement_data(
    #     df_sirets_acheteurs, ["enseigne1Etablissement"], "acheteur_id"
    # )

    print("Ajout des données unités légales (acheteurs)...")
    df = add_unite_legale_data(
        df, df_sirets_acheteurs, siret_column="acheteur_id", type_siret="acheteur"
    )
    del df_sirets_acheteurs

    # print("Construction du champ acheteur_nom à partir des données SIRENE...")
    # df_sirets_acheteurs = make_acheteur_nom(df_sirets_acheteurs)

    # DONNÉES SIRENE TITULAIRES

    # Enrichissement des données pas prioritaire
    # cf https://github.com/ColinMaudry/decp-processing/issues/17

    print("Extraction des SIRET des titulaires...")
    df_sirets_titulaires = extract_unique_titulaires_siret(df)

    # print("Ajout des données établissements (titulaires)...")
    # df_sirets_titulaires = add_etablissement_data_to_titulaires(df_sirets_titulaires)

    print("Ajout des données unités légales (titulaires)...")
    df = add_unite_legale_data(
        df, df_sirets_titulaires, siret_column="titulaire_id", type_siret="titulaire"
    )
    del df_sirets_titulaires
    # print("Amélioration des données unités légales des titulaires...")
    # df_sirets_titulaires = improve_titulaire_unite_legale_data(df_sirets_titulaires)

    # print("Renommage de certaines colonnes unités légales (titulaires)...")
    # df_sirets_titulaires = rename_titulaire_sirene_columns(df_sirets_titulaires)

    # print("Jointure pour créer les données DECP Titulaires...")
    # df_decp_titulaires = merge_sirets_titulaires(df, df_sirets_titulaires)
    # del df_sirets_titulaires

    # print("Enregistrement des DECP Titulaires aux formats CSV et Parquet...")
    # save_to_files(df_decp_titulaires, f"{DIST_DIR}/decp-titulaires")
    # del df_decp_titulaires

    df = df.drop(cs.ends_with("_siren"))

    return df

import polars as pl
from os import getenv
from config import SIRENE_DATA_DIR


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
    etablissement_df_chunked = pl.scan_csv(
        getenv(f"{SIRENE_DATA_DIR}/etablissements.parquet"),
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

    unites_legales_lf = pl.scan_parquet(SIRENE_DATA_DIR + "/unites_legales.parquet")

    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données
    df_sirets = df_sirets.join(unites_legales_lf, how="inner", on="siren")
    df_sirets = df_sirets.rename(
        {"denominationUniteLegale": f"{type_siret}_nom", "siren": f"{type_siret}_siren"}
    )

    # Ajout des données acheteurs enrichies au df de base
    df = df.join(df_sirets, how="left", on=siret_column)

    # Si c'est des données titulaire non-SIRENE qui ont matché (rarissime), on ne peut pas garder les données ajoutées
    if type_siret == "titulaire":
        df = df.with_columns(
            pl.when(pl.col.titulaire_typeIdentifiant != "SIRET")
            .then(pl.struct(titulaire_siren=None, titulaire_nom=None))
            .otherwise(pl.struct("titulaire_siren", "titulaire_nom"))
            .struct.unnest()
        )

    return df

import os

import polars as pl
import polars.selectors as cs
from httpx import get
from prefect import task

from config import DATA_DIR
from tasks.output import save_to_sqlite


def explode_titulaires(df: pl.LazyFrame):
    # Explosion des champs titulaires sur plusieurs lignes (un titulaire de marché par ligne)
    # et une colonne par champ

    # Structure originale
    # [{{"id": "abc", "typeIdentifiant": "SIRET"}}]

    # Explosion de la liste de titulaires en autant de nouvelles lignes
    df = df.explode("titulaires")

    # Renommage du premier objet englobant
    df = df.select(
        pl.col("*"),
        pl.col("titulaires")
        .struct.rename_fields(["titulaires.object"])
        .alias("titulaires_renamed"),
    )

    # Extraction du premier objet dans une nouvelle colonne
    df = df.unnest("titulaires_renamed")

    # Renommage des champs de l'objet titulaire
    df = df.select(
        pl.col("*"),
        pl.col("titulaires.object")
        .struct.rename_fields(["titulaire_typeIdentifiant", "titulaire_id"])
        .alias("titulaire"),
    )

    # Extraction de l'objet titulaire
    df = df.unnest("titulaire")

    # Suppression des anciennes colonnes
    df = df.drop(["titulaires", "titulaires.object"])

    # Cast l'identifiant en string
    df = df.with_columns(pl.col("titulaire_id").cast(pl.String))

    # Correction des cas où typeIdentifiant et id sont inversés:
    df = df.with_columns(
        [
            pl.when(pl.col("titulaire_typeIdentifiant").str.contains(r"[0-9]"))
            .then(pl.col("titulaire_id"))
            .otherwise(pl.col("titulaire_typeIdentifiant"))
            .alias("titulaire_typeIdentifiant"),
            pl.when(pl.col("titulaire_typeIdentifiant").str.contains(r"[0-9]"))
            .then(pl.col("titulaire_typeIdentifiant"))
            .otherwise(pl.col("titulaire_id"))
            .alias("titulaire_id"),
        ]
    )

    return df


def remove_modifications_duplicates(df):
    """On supprime les marches avec un suffixe correspondant à un autre marché"""
    if "modifications" not in df.collect_schema().names():
        return df
    # Index sans les suffixes
    df_cleaned = remove_suffixes_from_uid_column(df)
    df_cleaned = df_cleaned.with_columns(
        modifications_len=pl.col("modifications").list.len(),
    )

    df_cleaned = df_cleaned.sort("modifications_len").unique("uid", keep="last")
    return df_cleaned


def remove_suffixes_from_uid_column(df):
    """Supprimer les suffixes des uid quand ce suffixe correspond au nombre de mofifications apportées au marché.
    Exemple : uid = [acheteur_id]12302 et le marché a deux modifications. uid => [acheteur_id]123.
    """
    df = df.with_columns(
        expected_suffix=pl.col("modifications").list.len().cast(pl.Utf8).str.zfill(2)
    )
    df = df.with_columns(
        uid=pl.when(pl.col("uid").str.ends_with(pl.col("expected_suffix")))
        .then(pl.col("uid").str.head(-2))
        .otherwise(pl.col("uid"))
    )
    return df


def replace_with_modification_data(lf: pl.LazyFrame):
    """
    Gère les modifications dans le DataFrame des DECP.
    Cette fonction extrait les informations des modifications et les fusionne avec le DataFrame de base en ajoutant une ligne par modification
    (chaque ligne contient les informations complètes à jour à la date de notification)
    Elle ajoute également la colonne "donneesActuelles" pour indiquer si la notification est la plus récente.
    """

    # Étape 1: Créer une copie du DataFrame initial sans la colonne "modifications"
    lf_base = lf.select(pl.all().exclude("modifications"))

    # Étape 2: Explode le DataFrame pour avoir une ligne par modification
    lf_exploded = (
        lf.select("uid", "modifications")
        .explode("modifications")
        .drop_nulls()
        .with_columns(pl.col("modifications").struct.field("modification"))
    )

    # Étape 3: Extraire les données des modifications
    lf_mods = lf_exploded.select(
        "uid",
        pl.col("modification")
        .struct.field("dateNotificationModification")
        .alias("dateNotification"),
        pl.col("modification")
        .struct.field("datePublicationDonneesModification")
        .alias("datePublicationDonnees"),
        pl.col("modification").struct.field("montant").alias("montant"),
        pl.col("modification").struct.field("dureeMois").alias("dureeMois"),
        pl.col("modification").struct.field("titulaires").alias("titulaires"),
    )

    # Étape 4: Joindre les données de base pour chaque ligne de modification
    lf_concat = (
        pl.concat(
            [
                lf_base.select(
                    "uid",
                    "dateNotification",
                    "datePublicationDonnees",
                    "montant",
                    "dureeMois",
                    "titulaires",
                ),
                lf_mods,
            ],
            how="vertical_relaxed",
        )
        .with_columns(
            pl.col("dateNotification")
            .rank(method="ordinal")
            .over("uid")
            .cast(pl.Int64)
            .sub(1)
            .alias("modification_id")
        )
        .with_columns(
            (
                pl.col("modification_id") == pl.col("modification_id").max().over("uid")
            ).alias("donneesActuelles")
        )
        .sort(
            ["uid", "dateNotification", "modification_id"],
            descending=[False, True, True],
        )
    )

    # Étape 5: Remplir les valeurs nulles en utilisant les dernières valeurs non-nulles pour chaque id
    lf_concat = lf_concat.with_columns(
        pl.col("montant", "dureeMois", "titulaires")
        .fill_null(strategy="backward")
        .over("uid")
    )

    # Étape 5: Ajouter les données du DataFrame de base
    lf_final = lf_concat.join(
        lf.drop(
            [
                "dateNotification",
                "datePublicationDonnees",
                "montant",
                "dureeMois",
                "titulaires",
                "modifications",
            ]
        ),
        on="uid",
        how="left",
    )

    return lf_final


def process_modifications(lf: pl.LazyFrame):
    # Pas encore au point, risque de trop gros effets de bord
    # lf = remove_modifications_duplicates(lf)

    lf = replace_with_modification_data(lf)

    # Si il n'y avait aucun modifications dans le fichier, il faut ajouter les colonnes
    if "donneesActuelles" not in lf.collect_schema():
        lf = lf.with_columns(
            pl.lit(0).alias("modification_id"), pl.lit(True).alias("donneesActuelles")
        )

    return lf


def normalize_tables(df: pl.DataFrame):
    # MARCHES

    df_marches: pl.DataFrame = df.drop("titulaire_id", "titulaire_typeIdentifiant")
    df_marches = df_marches.unique(subset=["uid", "modification_id"]).sort(
        by="datePublicationDonnees", descending=True
    )
    save_to_sqlite(df_marches, "datalab", "marches", "uid, modification_id")
    del df_marches

    # ACHETEURS

    df_acheteurs: pl.DataFrame = df.select(cs.starts_with("acheteur"))
    df_acheteurs = df_acheteurs.rename(lambda name: name.removeprefix("acheteur_"))
    df_acheteurs = df_acheteurs.unique().sort(by="id")
    save_to_sqlite(df_acheteurs, "datalab", "acheteurs", "id")
    del df_acheteurs

    # TITULAIRES

    ## Table entreprises
    df_titulaires: pl.DataFrame = df.select(cs.starts_with("titulaire"))

    ### On garde les champs id et typeIdentifiant en clé primaire composite
    df_titulaires = df_titulaires.rename(lambda name: name.removeprefix("titulaire_"))
    df_titulaires = df_titulaires.unique().sort(by=["id"])
    save_to_sqlite(df_titulaires, "datalab", "entreprises", "id, typeIdentifiant")
    del df_titulaires

    ## Table marches_titulaires
    df_marches_titulaires: pl.DataFrame = df.select(
        "uid", "titulaire_id", "titulaire_typeIdentifiant", "modification_id"
    )
    df_marches_titulaires = df_marches_titulaires.rename(
        {"uid": "marche_uid", "modification_id": "marche_modification_id"}
    )
    save_to_sqlite(
        df_marches_titulaires,
        "datalab",
        "marches_titulaires",
        '"marche_uid", "titulaire_id", "titulaire_typeIdentifiant", "marche_modification_id"',
    )
    del df_marches_titulaires

    # TODO ajouter les sous-traitants quand ils seront ajoutés aux données


def concat_decp_json(dfs: list) -> pl.DataFrame:
    df = pl.concat(dfs, how="diagonal_relaxed")

    del dfs

    print(
        "Suppression des lignes en doublon par UID + titulaire ID + titulaire type ID + modification_id"
    )

    # Exemple : 20005584600014157140791205100
    index_size_before = df.height
    df = df.unique(
        subset=["uid", "titulaire_id", "titulaire_typeIdentifiant", "modification_id"],
        maintain_order=False,
    )
    print("-- ", index_size_before - df.height, " doublons supprimés")

    return df


def setup_tableschema_columns(df: pl.DataFrame):
    # Ajout colonnes manquantes
    df = df.with_columns(pl.lit("").alias("lieuExecution_nom"))  # TODO
    df = df.with_columns(pl.lit("").alias("objetModification"))  # TODO
    df = df.with_columns(pl.lit("").alias("donneesActuelles"))  # TODO
    df = df.with_columns(pl.lit("").alias("anomalies"))  # TODO

    tableschema = get(
        "https://raw.githubusercontent.com/ColinMaudry/decp-table-schema/refs/heads/main/schema.json",
        follow_redirects=True,
    ).json()
    fields = [field["name"] for field in tableschema["fields"]]
    df = df.select(fields)

    return df


def extract_unique_acheteurs_siret(df: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    df = df.select("acheteur_id")
    df = df.unique().filter(pl.col("acheteur_id") != "")
    df = df.sort(by="acheteur_id")
    return df


def extract_unique_titulaires_siret(df: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    df = df.select("titulaire_id", "titulaire_typeIdentifiant")
    df = df.unique().filter(
        pl.col("titulaire_id") != "", pl.col("titulaire_typeIdentifiant") == "SIRET"
    )
    df = df.sort(by="titulaire_id")
    return df


@task
def get_prepare_unites_legales(processed_parquet_path):
    print("Téléchargement des données unité légales et sélection des colonnes...")
    (
        pl.scan_parquet(os.environ["SIRENE_UNITES_LEGALES_URL"])
        .filter(pl.col("siren").is_not_null())
        .filter(pl.col("denominationUniteLegale").is_not_null())
        .sort("dateDebut", descending=False)
        .unique(subset=["siren"], keep="last")
        .select(["siren", "denominationUniteLegale"])
        .sink_parquet(processed_parquet_path)
    )


def sort_columns(df: pl.DataFrame, config_columns):
    # Les colonnes présentes mais absentes des colonnes attendues sont mises à la fin de la liste
    other_columns = []
    for col in df.columns:
        if col not in config_columns:
            other_columns.append(col)

    print("Colonnes inattendues:", other_columns)

    return df.select(config_columns + other_columns)


#
# ⬇️⬇️⬇️ Fonctions à refactorer avec Polars et le format DECP 2022 ⬇️⬇️⬇️
#


def make_acheteur_nom(decp_acheteurs_df: pl.LazyFrame):
    # Construction du champ acheteur_id

    from numpy import nan as NaN

    def construct_nom(row):
        if row["enseigne1Etablissement"] is NaN:
            return row["denominationUniteLegale"]
        else:
            return f"{row['denominationUniteLegale']} - {row['enseigne1Etablissement']}"

    decp_acheteurs_df["acheteur_id"] = decp_acheteurs_df.apply(construct_nom, axis=1)

    # TODO: ne garder que les colonnes acheteur_id et acheteur_id

    return decp_acheteurs_df


def improve_titulaire_unite_legale_data(df_sirets_titulaires: pl.DataFrame):
    # Raccourcissement du code commune
    df_sirets_titulaires["departement"] = df_sirets_titulaires[
        "codeCommuneEtablissement"
    ].str[:2]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["codeCommuneEtablissement"]
    )

    # # Raccourcissement de l'activité principale
    # pas sûr de pourquoi je voulais raccourcir le code NAF/APE. Pour récupérérer des libellés ?
    # decp_titulaires_sirets_df['activitePrincipaleEtablissement'] = decp_titulaires_sirets_df['activitePrincipaleEtablissement'].str[:-3]

    # Correction des données ESS et état
    df_sirets_titulaires["etatAdministratifUniteLegale"] = df_sirets_titulaires[
        "etatAdministratifUniteLegale"
    ].cat.rename_categories({"A": "Active", "C": "Cessée"})
    df_sirets_titulaires["economieSocialeSolidaireUniteLegale"] = df_sirets_titulaires[
        "economieSocialeSolidaireUniteLegale"
    ].replace({"O": "Oui", "N": "Non"})

    df_sirets_titulaires = improve_categories_juridiques(df_sirets_titulaires)

    return df_sirets_titulaires


def improve_categories_juridiques(df_sirets_titulaires: pl.DataFrame):
    # Récupération et raccourcissement des categories juridiques du fichier SIREN
    df_sirets_titulaires["categorieJuridiqueUniteLegale"] = (
        df_sirets_titulaires["categorieJuridiqueUniteLegale"].astype(str).str[:2]
    )

    # Récupération des libellés des catégories juridiques
    cj_df = pl.read_csv(DATA_DIR / "cj.csv", index_col=None, dtype="object")
    df_sirets_titulaires = pl.merge(
        df_sirets_titulaires,
        cj_df,
        how="left",
        left_on="categorieJuridiqueUniteLegale",
        right_on="Code",
    )
    df_sirets_titulaires["categorieJuridique"] = df_sirets_titulaires["Libellé"]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["Code", "categorieJuridiqueUniteLegale", "Libellé"]
    )
    return df_sirets_titulaires


def rename_titulaire_sirene_columns(df_sirets_titulaires: pl.DataFrame):
    # Renommage des colonnes

    renaming = {
        "activitePrincipaleEtablissement": "codeAPE",
        "etatAdministratifUniteLegale": "etatEntreprise",
        "etatAdministratifEtablissement": "etatEtablissement",
    }

    df_sirets_titulaires = df_sirets_titulaires.rename(columns=renaming)

    return df_sirets_titulaires

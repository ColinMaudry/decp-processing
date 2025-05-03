import polars as pl
from httpx import get
from prefect import task
from config import SIRENE_DATA_DIR
import os

from tasks.output import save_to_sqlite
import zipfile


def explode_titulaires(df: pl.DataFrame):
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

    return df


def normalize_tables(df):
    # MARCHES

    df_marches: pl.DataFrame = pl.DataFrame(df.to_arrow()).drop(
        "titulaire_id", "titulaire_typeIdentifiant"
    )
    df_marches = df_marches.unique("uid").sort(
        by="datePublicationDonnees", descending=True
    )
    save_to_sqlite(df_marches, "datalab", "marches", "uid")
    del df_marches

    # ACHETEURS

    df_acheteurs: pl.DataFrame = df.select("acheteur_id")
    df_acheteurs = df_acheteurs.rename({"acheteur_id": "id"})
    df_acheteurs = df_acheteurs.unique().sort(by="id")
    save_to_sqlite(df_acheteurs, "datalab", "acheteurs", "id")
    del df_acheteurs

    # TITULAIRES

    ## Table entreprises
    df_titulaires: pl.DataFrame = df.select("titulaire_id", "titulaire_typeIdentifiant")

    ### On garde les champs id et typeIdentifiant en clé primaire composite
    df_titulaires = df_titulaires.rename(
        {"titulaire_id": "id", "titulaire_typeIdentifiant": "typeIdentifiant"}
    )
    df_titulaires = df_titulaires.unique().sort(by=["id"])
    save_to_sqlite(df_titulaires, "datalab", "entreprises", "id, typeIdentifiant")
    del df_titulaires

    ## Table marches_titulaires
    df_marches_titulaires: pl.DataFrame = df.select(
        "uid", "titulaire_id", "titulaire_typeIdentifiant"
    )
    df_marches_titulaires = df_marches_titulaires.rename({"uid": "marche_uid"})
    save_to_sqlite(
        df_marches_titulaires,
        "datalab",
        "marches_titulaires",
        '"marche_uid", "titulaire_id", "titulaire_typeIdentifiant"',
    )
    del df_marches_titulaires

    # TODO ajouter les sous-traitants quand ils seront ajoutés aux données


def concat_decp_json(files: list) -> pl.DataFrame:
    dfs = []
    for file in files:
        df: pl.DataFrame = pl.read_parquet(f"{file}.parquet")
        dfs.append(df)

    df = pl.concat(dfs, how="diagonal")

    print(
        "Suppression des lignes en doublon par UID + titulaire ID + titulaire type ID"
    )
    # Exemple : 20005584600014157140791205100
    index_size_before = df.height
    df = df.unique(
        subset=["uid", "titulaire_id", "titulaire_typeIdentifiant"],
        maintain_order=False,
    )
    print("-- ", index_size_before - df.height, " doublons supprimés")

    return df


def setup_tableschema_columns(df: pl.DataFrame):
    # Ajout colonnes manquantes
    df = df.with_columns(pl.lit("").alias("acheteur_nom"))  # TODO
    df = df.with_columns(pl.lit("").alias("titulaire_denominationSociale"))  # TODO
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


def make_decp_sans_titulaires(df: pl.DataFrame):
    df_decp_sans_titulaires = df.drop(
        [
            "titulaire_id",
            "titulaire_typeIdentifiant",
        ]
    )
    df_decp_sans_titulaires = df_decp_sans_titulaires.unique()
    return df_decp_sans_titulaires


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
def get_prepare_unites_legales():
    sirene_data_dir = SIRENE_DATA_DIR

    unites_legales_path = f"{sirene_data_dir}/StockUniteLegale_utf8"
    if not os.path.exists(f"{unites_legales_path}.zip"):
        print("Téléchargement des unités légales...")
        unites_legales_url = os.getenv("SIRENE_UNITES_LEGALES_URL")

        request = get(unites_legales_url, follow_redirects=True)
        with open(f"{unites_legales_path}.zip", "wb") as file:
            file.write(request.content)

    if not os.path.exists(f"{unites_legales_path}.csv"):
        print("Décompression des unités légales...")
        with zipfile.ZipFile(f"{unites_legales_path}.zip", "r") as zip_ref:
            zip_ref.extractall(sirene_data_dir)

    print("-- sélection des colonnes et enregistrement au format parquet...")
    lf_ul = pl.scan_csv(f"{unites_legales_path}.csv", infer_schema=None)
    lf_ul = lf_ul.select(["siren", "denominationUniteLegale"])
    lf_ul = lf_ul.sort(by="siren")
    lf_ul.collect(engine="streaming").write_parquet(
        f"{sirene_data_dir}/unites_legales.parquet"
    )


def sort_columns(df: pl.DataFrame, config_columns):
    # Les colonnes présentes mais absentes des colonnes attendues sont mises à la fin de la liste
    other_columns = []
    for col in df.columns:
        if not col in config_columns:
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
            return f'{row["denominationUniteLegale"]} - {row["enseigne1Etablissement"]}'

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
    cj_df = pl.read_csv("data/cj.csv", index_col=None, dtype="object")
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

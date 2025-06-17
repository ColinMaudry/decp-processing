import shutil

import polars as pl
from prefect import flow, task

from config import BASE_DF_COLUMNS, DECP_PROCESSING_PUBLISH, DIST_DIR
from tasks.analyse import generate_stats
from tasks.clean import clean_decp_json
from tasks.enrich import add_unite_legale_data
from tasks.get import get_decp_json
from tasks.output import (
    save_to_files,
    save_to_sqlite,
)
from tasks.publish import publish_to_datagouv
from tasks.transform import (
    concat_decp_json,
    extract_unique_acheteurs_siret,
    extract_unique_titulaires_siret,
    get_prepare_unites_legales,
    make_decp_sans_titulaires,
    normalize_tables,
    sort_columns,
)


@task(log_prints=True)
def get_clean_concat():
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir(exist_ok=True)

    print("Récupération des données source...")
    files = get_decp_json()

    print("Nettoyage des données source et typage des colonnes...")
    files = clean_decp_json(files)

    print("Fusion des dataframes...")
    df = concat_decp_json(files)

    print("Ajout des données SIRENE...")
    lf: pl.LazyFrame = enrich_from_sirene(df.lazy())

    print("Génération de l'artefact (statistiques) sur le base df...")
    df: pl.DataFrame = lf.collect(engine="streaming")
    generate_stats(df)

    print("Enregistrement des DECP aux formats CSV, Parquet...")
    df: pl.DataFrame = sort_columns(df, BASE_DF_COLUMNS)
    save_to_files(df, DIST_DIR / "decp")


@flow(log_prints=True)
def make_datalab_data():
    """Tâches consacrées à la transformation des données dans un format
    adapté aux activités du Datalab d'Anticor."""

    df: pl.DataFrame = pl.read_parquet(DIST_DIR / "decp.parquet")

    print("Enregistrement des DECP aux formats SQLite...")
    save_to_sqlite(
        df,
        "datalab",
        "data.gouv.fr.2022.clean",
        "uid, titulaire_id, titulaire_typeIdentifiant",
    )

    print("Normalisation des tables...")
    normalize_tables(df)

    if DECP_PROCESSING_PUBLISH.lower() == "true":
        print("Publication sur data.gouv.fr...")
        publish_to_datagouv(context="datalab")
    else:
        print("Publication sur data.gouv.fr désactivée.")


@flow(log_prints=True)
def make_decpinfo_data():
    """Tâches consacrées à la transformation des données dans un format
    # adapté à decp.info"""

    df: pl.DataFrame = pl.read_parquet(DIST_DIR / "decp.parquet")

    # DECP sans titulaires
    save_to_files(make_decp_sans_titulaires(df), DIST_DIR / "decp-sans-titulaires")

    # print("Ajout des colonnes manquantes...")
    # df = setup_tableschema_columns(df)

    # CREATION D'UN DATA PACKAGE (FRICTIONLESS DATA)

    # Pas la priorité pour le moment, prend du temps
    # print("Validation des données DECP avec le TableSchema...")
    # validate_decp_against_tableschema()

    # print("Création du data package (JSON)....")
    # make_data_package()

    # PUBLICATION DES FICHIERS SUR DATA.GOUV.FR
    if DECP_PROCESSING_PUBLISH.lower() == "true":
        print("Publication sur data.gouv.fr...")
        publish_to_datagouv(context="decp")
    else:
        print("Publication sur data.gouv.fr désactivée.")

    return df


@flow(log_prints=True)
def decp_processing():
    # Données nettoyées et fusionnées
    get_clean_concat()

    # Fichiers dédiés à l'Open Data et decp.info
    make_decpinfo_data()

    # Base de données SQLite dédiée aux activités du Datalab d'Anticor
    make_datalab_data()


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

    # print("Construction du champ acheteur_nom à partir des données SIRENE...")
    # df_sirets_acheteurs = make_acheteur_nom(df_sirets_acheteurs)

    # print("Enregistrement des DECP aux formats CSV et Parquet...")
    # save_to_files(df, f"{DIST_DIR}/decp")

    # print("Suppression de colonnes et déduplication pour les DECP Sans Titulaires...")
    # df_decp_sans_titulaires = make_decp_sans_titulaires(df)
    # save_to_files(df_decp_sans_titulaires, f"{DIST_DIR}/decp-sans-titulaires")
    # del df_decp_sans_titulaires

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

    return df


@flow(log_prints=True)
def sirene_preprocess():
    """Prétraitement mensuel des données SIRENE afin d'économiser du temps lors du traitement quotidien des DECP.
    Pour chaque ressource (unités légales, établissements), un fichier parquet est produit.
    """
    # préparer lest données établissements

    # préparer les données unités légales
    print("Prépararion des unités légales...")
    get_prepare_unites_legales()


if __name__ == "__main__":
    decp_processing()

from prefect import flow
from datetime import datetime
from dotenv import load_dotenv

from tasks.get import *
from tasks.clean import *
from tasks.transform import *
from tasks.output import *
from tasks.analyse import list_data_issues
from tasks.setup import *

# from tasks.test import *
# from tasks.enrich import *

load_dotenv()

CONNS = {}
for db in ["datalab", "decp"]:
    CONNS[db] = create_engine(f"sqlite:///dist/{db}.sqlite", echo=False)

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD


@flow(log_prints=True)
def get_merge_clean():
    print("Récupération des données source...")
    df: pl.DataFrame = get_merge_decp(DATE_NOW)
    print(f"DECP officielles: nombre de lignes: {df.height}")
    save_to_sqlite(df, "datalab", "data.economie.2019.2022")

    print("Nettoyage des données source...")
    df = clean_decp(df)

    print("Typage des colonnes...")
    df = fix_data_types(df)

    print("Problèmes dans les données...")
    list_data_issues(df)

    return df


@flow(log_prints=True)
def make_datalab_data():
    """Tâches consacrées à la transformation des données dans un format
    adapté aux activités du Datalab d'Anticor."""

    # Initialisation
    initialization()

    # Récupération des données
    df: pl.DataFrame = get_merge_clean()

    print("Enregistrement des DECP aux formats CSV, Parquet et SQLite...")
    save_to_files(df, "dist/decp")
    save_to_sqlite(df, "datalab", "data.economie.2019.2022.clean")

    return df


@flow(log_prints=True)
def make_decpinfo_data():
    # Tâches consacrées à la transformation des données dans un format
    # adapté à decp.info (datasette)

    # Récupération des données
    df: pl.DataFrame = get_merge_clean()

    print("Concaténation et explosion des titulaires, un par ligne...")
    df = explode_titulaires(df)

    # print("Ajout des colonnes manquantes...")
    df = setup_tableschema_columns(df)

    # Ajout des données de la base SIRENE
    df = enrich_from_sirene(df)

    # CREATION D'UN DATA PACKAGE (FRICTIONLESS DATA) ET DES FICHIERS DATASETTE

    # if not (os.curdir.endswith("dist")):
    #     os.chdir("./dist")
    #     print(os.curdir)
    #
    # print("Validation des données DECP avec le TableSchema...")
    # validate_decp_against_tableschema()
    #
    # print("Création du data package (JSON)....")
    # make_data_package()
    #
    # print("Création de la DB SQLite et des métadonnées datasette...")
    # make_sqllite_and_datasette_metadata()

    # PUBLICATION DES FICHIERS SUR DATA.GOUV.FR

    return df


@flow(log_prints=True)
def enrich_from_sirene(df):
    # DONNÉES SIRENE ACHETEURS

    # Enrichissement des données pas prioritaire
    # cf https://github.com/ColinMaudry/decp-processing/issues/17

    # print("Extraction des SIRET des acheteurs...")
    # df_sirets_acheteurs = extract_unique_acheteurs_siret(df)

    # print("Ajout des données établissements (acheteurs)...")
    # df_sirets_acheteurs = add_etablissement_data_to_acheteurs(df_sirets_acheteurs)

    # print("Ajout des données unités légales (acheteurs)...")
    # df_sirets_acheteurs = add_unite_legale_data_to_acheteurs(df_sirets_acheteurs)

    # print("Construction du champ acheteur.nom à partir des données SIRENE...")
    # df_sirets_acheteurs = make_acheteur_nom(df_sirets_acheteurs)

    # print("Jointure des données acheteurs enrichies avec les DECP...")
    # df = merge_sirets_acheteurs(df, df_sirets_acheteurs)
    # del df_sirets_acheteurs

    # print("Enregistrement des DECP aux formats CSV et Parquet...")
    # save_to_files(df, "dist/decp")

    # print("Suppression de colonnes et déduplication pour les DECP Sans Titulaires...")
    # df_decp_sans_titulaires = make_decp_sans_titulaires(df)
    # save_to_files(df_decp_sans_titulaires, "dist/decp-sans-titulaires")
    # del df_decp_sans_titulaires

    # DONNÉES SIRENE TITULAIRES

    # Enrichissement des données pas prioritaire
    # cf https://github.com/ColinMaudry/decp-processing/issues/17

    # print("Extraction des SIRET des titulaires...")
    # df_sirets_titulaires = extract_unique_titulaires_siret(df)

    # print("Ajout des données établissements (titulaires)...")
    # df_sirets_titulaires = add_etablissement_data_to_titulaires(df_sirets_titulaires)

    # print("Ajout des données unités légales (titulaires)...")
    # df_sirets_titulaires = add_unite_legale_data_to_titulaires(df_sirets_titulaires)

    # print("Amélioration des données unités légales des titulaires...")
    # df_sirets_titulaires = improve_titulaire_unite_legale_data(df_sirets_titulaires)

    # print("Renommage de certaines colonnes unités légales (titulaires)...")
    # df_sirets_titulaires = rename_titulaire_sirene_columns(df_sirets_titulaires)

    # print("Jointure pour créer les données DECP Titulaires...")
    # df_decp_titulaires = merge_sirets_titulaires(df, df_sirets_titulaires)
    # del df_sirets_titulaires

    # print("Enregistrement des DECP Titulaires aux formats CSV et Parquet...")
    # save_to_files(df_decp_titulaires, "dist/decp-titulaires")
    # del df_decp_titulaires

    return df


if __name__ == "__main__":
    make_datalab_data()

    # On verra les deployments quand la base marchera
    #
    # decp_processing.serve(
    #     name="decp-processing-cron",
    #     cron="0 6 * * 1-5",
    #     description="Téléchargement, traitement, et publication des DECP.",
    # )
    # decp_processing.serve(
    #     name="decp-processing-once",
    #     description="Téléchargement, traitement, et publication des DECP.",
    # )

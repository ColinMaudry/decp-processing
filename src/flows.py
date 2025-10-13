import datetime
import os
import shutil
from time import sleep

import polars as pl
from prefect import flow, task
from prefect.artifacts import create_table_artifact
from prefect.task_runners import ConcurrentTaskRunner
from prefect.transactions import transaction
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

from config import (
    BASE_DF_COLUMNS,
    BASE_DIR,
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    MARCHES_SECURISES_SCRAPING_MODE,
    MAX_PREFECT_WORKERS,
    MONTH_NOW,
    SIRENE_DATA_DIR,
    TRACKED_DATASETS,
)
from tasks.clean import clean_decp
from tasks.dataset_utils import list_resources
from tasks.enrich import enrich_from_sirene
from tasks.get import get_resource, scrap_marches_securises_month
from tasks.output import generate_final_schema, save_to_databases, save_to_files
from tasks.publish import publish_to_datagouv
from tasks.transform import (
    concat_decp_json,
    get_prepare_unites_legales,
    normalize_tables,
    sort_columns,
)
from tasks.utils import (
    create_sirene_data_dir,
    generate_stats,
    get_clean_cache_key,
    remove_unused_cache,
)


@task(
    log_prints=True,
    persist_result=True,
    cache_expiration=datetime.timedelta(hours=CACHE_EXPIRATION_TIME_HOURS),
    cache_key_fn=get_clean_cache_key,
)
def get_clean(resource, resources_artifact: list) -> pl.DataFrame or None:
    # Récupération des données source...
    with transaction():
        lf, decp_format = get_resource(resource, resources_artifact)

        # Nettoyage des données source et typage des colonnes...
        # si la ressource est dans un format supporté
        if lf is not None:
            lf = clean_decp(lf, decp_format)
            df = lf.collect(engine="streaming")

    return df


@task(log_prints=True)
def make_data_tables():
    """Tâches consacrées à la transformation des données dans un format relationnel (SQL)."""

    print("Création de la base données au format relationnel...")

    lf: pl.LazyFrame = pl.scan_parquet(DIST_DIR / "decp.parquet")

    print("Enregistrement des DECP (base DataFrame) dans les bases de données...")
    print(DIST_DIR / "decp.parquet")
    save_to_databases(
        lf,
        "decp",
        "data.gouv.fr.2022.clean",
        "uid, titulaire_id, titulaire_typeIdentifiant, modification_id",
    )

    print("Normalisation des tables...")
    normalize_tables(lf)


@flow(
    log_prints=True,
    task_runner=ConcurrentTaskRunner(max_workers=MAX_PREFECT_WORKERS),
)
def decp_processing(enable_cache_removal: bool = False):
    print(f"🚀  Début du flow decp-processing dans base dir {BASE_DIR} ")

    print("Liste de toutes les ressources des datasets...")
    resources: list[dict] = list_resources(TRACKED_DATASETS)

    # Initialisation du tableau des artifacts de ressources
    resources_artifact = []

    # Traitement parallèle des ressources
    futures = [
        get_clean.submit(resource, resources_artifact)
        for resource in resources
        if resource["filesize"] > 100
    ]
    dfs: list[pl.DataFrame] = [f.result() for f in futures if f.result() is not None]

    if DECP_PROCESSING_PUBLISH:
        create_table_artifact(
            table=resources_artifact,
            key="datagouvfr-json-resources",
            description=f"Les ressources utilisées comme source ({DATE_NOW})",
        )
        del resources_artifact

    print("Fusion des dataframes...")
    df: pl.DataFrame = concat_decp_json(dfs)

    print("Ajout des données SIRENE...")
    # Preprocessing des données SIRENE si :
    # - le dossier n'existe pas encore (= les données n'ont pas déjà été preprocessed ce mois-ci)
    # - on est au moins le 5 du mois (pour être sûr que les données SIRENE ont été mises à jour sur data.gouv.fr)
    if not SIRENE_DATA_DIR.exists() and int(DATE_NOW[-2:]) >= 5:
        sirene_preprocess()
    lf: pl.LazyFrame = enrich_from_sirene(df.lazy())

    df: pl.DataFrame = lf.collect(engine="streaming")

    # Réinitialisation de DIST_DIR
    if os.path.exists(DIST_DIR):
        shutil.rmtree(DIST_DIR)
    os.makedirs(DIST_DIR)

    if DECP_PROCESSING_PUBLISH:
        print("Génération de l'artefact (statistiques) sur le base df...")
        generate_stats(df)

    print("Génération du schéma et enregistrement des DECP aux formats CSV, Parquet...")
    df: pl.DataFrame = sort_columns(df, BASE_DF_COLUMNS)
    generate_final_schema(df)
    save_to_files(df, DIST_DIR / "decp")
    del df

    # Base de données SQLite dédiée aux activités du Datalab d'Anticor
    # Désactivé pour l'instant https://github.com/ColinMaudry/decp-processing/issues/124
    # make_data_tables()

    if DECP_PROCESSING_PUBLISH:
        print("Publication sur data.gouv.fr...")
        publish_to_datagouv()
    else:
        print("Publication sur data.gouv.fr désactivée.")

    # Suppression des fichiers de cache inutilisés
    if enable_cache_removal:
        remove_unused_cache()

    print("☑️  Fin du flow principal decp_processing.")


@flow(log_prints=True)
def sirene_preprocess():
    """Prétraitement mensuel des données SIRENE afin d'économiser du temps lors du traitement quotidien des DECP.
    Pour chaque ressource (unités légales, établissements), un fichier parquet est produit.
    """

    print("🚀  Pré-traitement des données SIRENE")
    # Soit les tâches de ce flow vont au bout (success), soit le dossier SIRENE_DATA_DIR est supprimé (voir remove_sirene_data_dir())
    with transaction():
        create_sirene_data_dir()

        # TODO préparer lest données établissements

        # préparer les données unités légales
        processed_parquet_path = SIRENE_DATA_DIR / "unites_legales.parquet"
        if not processed_parquet_path.exists():
            print("Prépararion des unités légales...")
            get_prepare_unites_legales(processed_parquet_path)

    print("☑️  Fin du flow sirene_preprocess.")


@flow(log_prints=True)
def scrap_marches_securises(mode=None, year=None):
    mode = mode or MARCHES_SECURISES_SCRAPING_MODE

    current_year = DATE_NOW[:4]

    if mode == "month":
        scrap_marches_securises_month(current_year, MONTH_NOW)

    elif mode == "year":
        year = year or current_year
        for month in reversed(range(1, 13)):
            month = str(month).zfill(2)
            scrap_marches_securises_month(year, month)

    elif mode == "all":
        current_year = int(current_year)
        for year in reversed(range(2018, current_year + 2)):
            scrap_marches_securises("year", str(year))

    else:
        print("Mauvaise configuration")


@flow(log_prints=True)
def scrap_aws(mode=None, year=None):
    options = Options()
    options.add_argument("--headless")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", DIST_DIR)

    driver = webdriver.Firefox(options=options)
    driver.get("https://www.marches-publics.info/Annonces/rechercher")

    # Formulaire recherche données essentielles
    sleep(1)
    de_radio = driver.find_element(By.ID, "typeDE")
    de_radio.click()

    # Remplir le formulaire
    notif_debut = driver.find_element(By.ID, "dateNotifDebut")
    notif_debut.clear()
    notif_debut.send_keys("2025-01-01")
    notif_fin = driver.find_element(By.ID, "dateNotifFin")
    notif_fin.clear()
    notif_fin.send_keys("2025-01-03")

    sleep(1)

    driver.find_element(By.ID, "sub").click()
    driver.find_element(By.ID, "downloadDonnees").click()
    # Le téléchargement se prépare et se lance en arrière plan
    sleep(3)
    # Le fichier donneesEssentielles.json est dans DIST_DIR, il faut le renommer avant de télécharger le suivant.

    driver.close()


if __name__ == "__main__":
    # decp_processing()
    scrap_marches_securises()

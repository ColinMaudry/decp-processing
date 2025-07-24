import datetime
import os
import shutil

import polars as pl
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.transactions import transaction

from config import (
    BASE_DF_COLUMNS,
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    MAX_PREFECT_WORKERS,
    SIRENE_DATA_DIR,
    TRACKED_DATASETS,
)
from tasks.clean import clean_decp
from tasks.dataset_utils import list_resources
from tasks.enrich import enrich_from_sirene
from tasks.get import get_resource
from tasks.output import (
    save_to_files,
    save_to_sqlite,
)
from tasks.publish import publish_to_datagouv
from tasks.transform import (
    concat_decp_json,
    get_prepare_unites_legales,
    normalize_tables,
    sort_columns,
)
from tasks.utils import create_sirene_data_dir, generate_stats, remove_unused_cache


def get_clean_cache_key(context, parameters) -> str:
    resource = parameters["resource"]
    # TOOD déplacer cette fonction vers tasks/utils.py

    # On utilise le hash sha1 de la ressource, généré par data.gouv.fr, comme clé de cache
    return resource["checksum"]


@task(
    log_prints=True,
    persist_result=True,
    cache_expiration=datetime.timedelta(hours=CACHE_EXPIRATION_TIME_HOURS),
    cache_key_fn=get_clean_cache_key,
)
def get_clean(resource) -> pl.DataFrame or None:
    # Récupération des données source...
    with transaction():
        lf: pl.LazyFrame = get_resource(resource)
        df = None

        # Nettoyage des données source et typage des colonnes...
        # si la ressource est dans un format supporté
        if lf is not None:
            lf = clean_decp(lf)
            df = lf.collect(engine="streaming")

    return df


@task(log_prints=True)
def make_datalab_data():
    """Tâches consacrées à la transformation des données dans un format
    adapté aux activités du Datalab d'Anticor."""

    print("Création de la base données pour le Datalab d'Anticor...")

    df: pl.DataFrame = pl.read_parquet(DIST_DIR / "decp.parquet")

    print("Enregistrement des DECP (base DataFrame) au format SQLite...")
    save_to_sqlite(
        df,
        "datalab",
        "data.gouv.fr.2022.clean",
        "uid, titulaire_id, titulaire_typeIdentifiant, modification_id",
    )

    print("Normalisation des tables...")
    normalize_tables(df)

    if DECP_PROCESSING_PUBLISH.lower() == "true":
        print("Publication sur data.gouv.fr...")
        publish_to_datagouv(context="datalab")
    else:
        print("Publication sur data.gouv.fr désactivée.")


@flow(
    log_prints=True, task_runner=ConcurrentTaskRunner(max_workers=MAX_PREFECT_WORKERS)
)
def decp_processing(enable_cache_removal: bool = False):
    print("🚀  Début du flow decp-processing")

    print("Liste de toutes les ressources des datasets...")
    resources: list[dict] = list_resources(TRACKED_DATASETS)

    # Traitement parallèle des ressources
    futures = [get_clean.submit(resource) for resource in resources]
    dfs: list[pl.DataFrame] = [f.result() for f in futures if f.result() is not None]

    print("Fusion des dataframes...")
    df: pl.DataFrame = concat_decp_json(dfs)

    print("Ajout des données SIRENE...")
    # Preprocessing des données SIRENE si :
    # - le dossier n'existe pas encore (= les données n'ont pas déjà été preprocessed ce mois-ci)
    # - on est au moins le 5 du mois (pour être sûr que les données SIRENE ont été mises à jour sur data.gouv.fr)
    if not SIRENE_DATA_DIR.exists() and int(DATE_NOW[-2:]) >= 5:
        sirene_preprocess()
    lf: pl.LazyFrame = enrich_from_sirene(df.lazy())

    print("Génération de l'artefact (statistiques) sur le base df...")
    df: pl.DataFrame = lf.collect(engine="streaming")

    generate_stats(df)

    if os.path.exists(DIST_DIR):
        shutil.rmtree(DIST_DIR)
    os.makedirs(DIST_DIR)

    print("Enregistrement des DECP aux formats CSV, Parquet...")
    df: pl.DataFrame = sort_columns(df, BASE_DF_COLUMNS)
    save_to_files(df, DIST_DIR / "decp")

    # Base de données SQLite dédiée aux activités du Datalab d'Anticor
    make_datalab_data()

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


if __name__ == "__main__":
    decp_processing()

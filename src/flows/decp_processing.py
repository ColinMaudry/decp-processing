import os
import shutil

import polars as pl
from prefect import flow
from prefect.artifacts import create_table_artifact
from prefect.task_runners import ConcurrentTaskRunner

from config import (
    BASE_DF_COLUMNS,
    BASE_DIR,
    DATE_NOW,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    MAX_PREFECT_WORKERS,
    SIRENE_DATA_DIR,
    TRACKED_DATASETS,
)
from flows.sirene_preprocess import sirene_preprocess
from tasks.dataset_utils import list_resources
from tasks.enrich import enrich_from_sirene
from tasks.get import get_clean
from tasks.output import generate_final_schema, save_to_files
from tasks.publish import publish_to_datagouv
from tasks.transform import concat_decp_json, sort_columns
from tasks.utils import generate_stats, remove_unused_cache


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
    if not SIRENE_DATA_DIR.exists():
        sirene_preprocess()

    lf: pl.LazyFrame = enrich_from_sirene(df.lazy())

    df: pl.DataFrame = lf.collect(engine="streaming")

    # Réinitialisation de DIST_DIR
    if os.path.exists(DIST_DIR):
        shutil.rmtree(DIST_DIR)
    os.makedirs(DIST_DIR)

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

import os
import shutil
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import polars.selectors as cs
from prefect import flow, task
from prefect.artifacts import create_table_artifact
from prefect.context import get_run_context
from prefect_email import EmailServerCredentials, email_send_message

from src.config import (
    BASE_DF_COLUMNS,
    DATE_NOW,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    LOG_LEVEL,
    MAX_PREFECT_WORKERS,
    PREFECT_API_URL,
    RESOURCE_CACHE_DIR,
    SIRENE_DATA_DIR,
    SOLO_DATASETS,
    TRACKED_DATASETS,
)
from src.flows.sirene_preprocess import sirene_preprocess
from src.tasks.dataset_utils import list_resources
from src.tasks.enrich import add_duree_restante, add_type_marche, enrich_from_sirene
from src.tasks.get import get_clean
from src.tasks.output import generate_final_schema, sink_to_files
from src.tasks.publish import publish_to_datagouv, publish_to_s3
from src.tasks.transform import (
    calculate_naf_cpv_matching,
    concat_parquet_files,
    sort_columns,
    sort_modifications,
)
from src.tasks.utils import (
    full_resource_name,
    generate_stats,
    get_logger,
    print_all_config,
    remove_unused_cache,
)


@flow(log_prints=True)
def decp_processing(enable_cache_removal: bool = True):
    logger = get_logger(level=LOG_LEVEL)

    logger.info("🚀  Début du flow decp-processing")

    print_all_config()

    logger.info("Liste de toutes les ressources des datasets...")
    resources: list[dict] = list_resources(TRACKED_DATASETS)

    # Initialisation du tableau des artifacts de ressources
    resources_artifact = []

    # Liste des ressources en cache (checksums)
    available_parquet_files = set(os.listdir(RESOURCE_CACHE_DIR))

    # Traitement parallèle des ressources par lots pour éviter la surcharge mémoire
    batch_size = 100
    parquet_files = []

    # Filtrer les ressources à traiter, en ne gardant que les fichiers > 180 octets
    resources_to_process = [r for r in resources if r["filesize"] > 180]

    for i in range(0, len(resources_to_process), batch_size):
        process_batch(
            available_parquet_files,
            batch_size,
            i,
            parquet_files,
            resources_artifact,
            resources_to_process,
        )

    # Afin d'être sûr que je ne publie pas par erreur un jeu de données de test
    decp_publish = (
        DECP_PROCESSING_PUBLISH
        and len(resources_to_process) > 5000
        and SOLO_DATASETS == []
    )

    if decp_publish:
        create_table_artifact(
            table=resources_artifact,
            key="datagouvfr-json-resources",
            description=f"Les ressources utilisées comme source ({DATE_NOW})",
        )
        del resources_artifact

    # Réinitialisation de DIST_DIR
    if os.path.exists(DIST_DIR):
        shutil.rmtree(DIST_DIR)
    os.makedirs(DIST_DIR)

    logger.info("Concaténation des dataframes...")
    lf: pl.LazyFrame = concat_parquet_files(parquet_files)

    logger.info("Tri des modifications...")
    lf = sort_modifications(lf)

    logger.info("Ajout des données SIRENE...")
    # Preprocessing des données SIRENE si :
    # - le dossier n'existe pas encore (= les données n'ont pas déjà été preprocessed ce mois-ci)
    # - on est au moins le 5 du mois (pour être sûr que les données SIRENE ont été mises à jour sur data.gouv.fr)
    if not SIRENE_DATA_DIR.exists():
        sirene_preprocess()

    lf: pl.LazyFrame = enrich_from_sirene(lf)

    sink_to_files(lf, DIST_DIR / "decp", file_format="parquet")
    lf: pl.LazyFrame = pl.scan_parquet(DIST_DIR / "decp.parquet")

    logger.info("Ajout de la colonne 'dureeRestanteMois'...")
    lf = add_duree_restante(lf)

    logger.info("Ajout du type de marché...")
    lf = add_type_marche(lf)

    logger.info("Génération des probabilités NAF/CPV...")
    calculate_naf_cpv_matching(lf)
    lf = lf.drop(cs.starts_with("activite"))

    logger.info("Génération de l'artefact (statistiques) sur le base df...")
    generate_stats(lf)

    logger.info(
        "Génération du schéma et enregistrement des DECP aux formats CSV, Parquet..."
    )
    lf: pl.LazyFrame = sort_columns(lf, BASE_DF_COLUMNS)
    generate_final_schema(lf)
    sink_to_files(lf, DIST_DIR / "decp")

    # Base de données SQLite dédiée aux activités du Datalab d'Anticor
    # Désactivé pour l'instant https://github.com/ColinMaudry/decp-processing/issues/124
    # make_data_tables()

    if decp_publish:
        logger.info("Publication sur data.gouv.fr...")
        publish_to_datagouv()

        logger.info("Publication sur S3...")
        publish_to_s3(
            file=DIST_DIR / "decp.parquet", prefix=f"decp/{DATE_NOW}/decp.parquet"
        )
    else:
        logger.info("Publication sur data.gouv.fr désactivée.")

    if enable_cache_removal:
        logger.info("Suppression des fichiers de cache inutilisés...")
        remove_unused_cache()

    logger.info("☑️  Fin du flow principal decp_processing.")


@task(retries=2, timeout_seconds=1800)
def process_batch(
    available_parquet_files,
    batch_size,
    i,
    parquet_files,
    resources_artifact,
    resources_to_process,
):
    logger = get_logger(level=LOG_LEVEL)
    batch = resources_to_process[i : i + batch_size]
    logger.info(
        f"🗃️ Traitement du lot {i // batch_size + 1} / {len(resources_to_process) // batch_size + 1}"
    )
    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_PREFECT_WORKERS) as executor:
        for resource in batch:
            future = executor.submit(
                get_clean, resource, resources_artifact, available_parquet_files
            )
            futures[future] = full_resource_name(resource)

    for future in futures:
        try:
            result = future.result()
            if result is not None:
                parquet_files.append(result)
        except Exception as e:
            resource_name = futures[future]
            logger.error(
                f"❌ Erreur de traitement de {resource_name} ({type(e).__name__}):"
            )
            logger.info(e)
    # Nettoyage explicite
    futures.clear()


@sirene_preprocess.on_failure
@decp_processing.on_failure
def notify_exception_by_email(flow, flow_run, state):
    if PREFECT_API_URL:
        context = get_run_context()
        flow_run_name = context.flow_run.name
        email_server_credentials = EmailServerCredentials.load("email-notifier")
        message = (
            f"Your job {flow_run.name} entered {state.name} "
            f"with message:\n\n"
            f"See <https://{PREFECT_API_URL}/flow-runs/flow-run/{flow_run.id}>\n\n"
            f"Scheduled start: {flow_run.expected_start_time}"
        )

        email_send_message(
            email_server_credentials=email_server_credentials,
            subject=f"Flow run {flow_run_name!r} failed",
            msg=message,
            email_to=email_server_credentials.username,
        )

import polars as pl
from prefect import flow
from prefect.transactions import transaction

from src.config import LOG_LEVEL, SIRENE_DATA_DIR
from src.flows.get_cog import get_cog
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    get_from_s3,
    get_unite_legales,
)
from src.tasks.transform import prepare_etablissements
from src.tasks.utils import create_sirene_data_dir, get_logger


@flow(log_prints=True)
def sirene_preprocess():
    """Prétraitement mensuel des données SIRENE afin d'économiser du temps lors du traitement quotidien des DECP.
    Pour chaque ressource (unités légales, établissements), un fichier parquet est produit.
    """

    logger = get_logger(level=LOG_LEVEL)

    logger.info("🚀  Pré-traitement des données SIRENE")
    # Soit les tâches de ce flow vont au bout (success), soit le dossier SIRENE_DATA_DIR est supprimé (voir remove_sirene_data_dir())
    with transaction():
        create_sirene_data_dir()

        # Récupération et préparation des données du Code Officiel Géographique
        get_cog()

        # Récupération du cache de géolocalisations
        lf_siret_latlong = get_from_s3(key="siret_latlong.parquet", prefix="")

        if not isinstance(lf_siret_latlong, pl.LazyFrame):
            lf_siret_latlong = bootstrap_siret_latlong()

        # préparer les données unités légales
        processed_ul_parquet_path = SIRENE_DATA_DIR / "unites_legales.parquet"
        if not processed_ul_parquet_path.exists():
            logger.info("Téléchargement et préparation des unités légales...")
            get_unite_legales(processed_ul_parquet_path)
        else:
            logger.info(str(processed_ul_parquet_path) + " existe, skipping.")

        # préparer les données établissements
        processed_etab_parquet_path = SIRENE_DATA_DIR / "etablissements.parquet"
        if not processed_etab_parquet_path.exists():
            logger.info("Téléchargement et préparation des établissements...")
            lf = get_etablissements()
            prepare_etablissements(lf).sink_parquet(processed_etab_parquet_path)
        else:
            logger.info(str(processed_etab_parquet_path) + " existe, skipping.")

    logger.info("☑️  Fin du flow sirene_preprocess.")

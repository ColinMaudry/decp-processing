from prefect import flow
from prefect.transactions import transaction

from config import SIRENE_DATA_DIR
from tasks.get import get_etablissements
from tasks.transform import get_prepare_unites_legales, prepare_etablissements
from tasks.utils import create_sirene_data_dir


@flow(log_prints=True)
def sirene_preprocess():
    """Prétraitement mensuel des données SIRENE afin d'économiser du temps lors du traitement quotidien des DECP.
    Pour chaque ressource (unités légales, établissements), un fichier parquet est produit.
    """

    print("🚀  Pré-traitement des données SIRENE")
    # Soit les tâches de ce flow vont au bout (success), soit le dossier SIRENE_DATA_DIR est supprimé (voir remove_sirene_data_dir())
    with transaction():
        create_sirene_data_dir()

        # préparer les données unités légales
        processed_ul_parquet_path = SIRENE_DATA_DIR / "unites_legales.parquet"
        if not processed_ul_parquet_path.exists():
            print("Prépararion des unités légales...")
            get_prepare_unites_legales(processed_ul_parquet_path)

        # préparer les données établissements
        processed_etab_parquet_path = SIRENE_DATA_DIR / "etablissements.parquet"
        if not processed_etab_parquet_path.exists():
            print("Téléchargement et préparation des établissements...")
            lf = get_etablissements()
            prepare_etablissements(lf, processed_etab_parquet_path)

    print("☑️  Fin du flow sirene_preprocess.")

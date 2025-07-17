import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from dotenv import find_dotenv, load_dotenv


def make_dirs_if_not_exist(path: str or Path) -> None:
    # os.makedirs() est en erreur si le dossier existe déjà
    # Path().mkdir ne peut créer qu'un dossier à la fois
    # donc...
    if not os.path.exists(path):
        os.makedirs(path)


dotenv_path = find_dotenv()
if dotenv_path == "":
    print("Création du fichier .env à partir de template.env")
    template_dotenv_path = Path(find_dotenv("template.env"))
    dotenv_path = template_dotenv_path.with_name(".env")
    shutil.copyfile(template_dotenv_path, dotenv_path)

load_dotenv(dotenv_path, override=False)

FORMAT_DETECTION_QUORUM = 0.7  # Seuil de confiance pour la détection du format des données - en pratique c'est inutile car il n'y a qu'un format par fichier

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]

DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

BASE_DIR = Path(__file__).parent.parent
CACHE_DIR = BASE_DIR / ".prefect" / "results"
REMOVE_UNUSED_CACHE_AFTER_DAYS = 15

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
make_dirs_if_not_exist(DATA_DIR)

DIST_DIR = Path(os.getenv("DECP_DIST_DIR", BASE_DIR / "dist"))
make_dirs_if_not_exist(DIST_DIR)

sirene_data_parent_dir = Path(os.getenv("SIRENE_DATA_PARENT_DIR", DATA_DIR))
SIRENE_DATA_DIR = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"
make_dirs_if_not_exist(SIRENE_DATA_DIR)

PREFECT_CACHE_DIR = Path(os.getenv("PREFECT_CACHE_DIR"))
make_dirs_if_not_exist(PREFECT_CACHE_DIR)
PREFECT_RESULTS_DIR = Path(os.getenv("PREFECT_RESULTS_DIR"))
make_dirs_if_not_exist(PREFECT_RESULTS_DIR)


with open(
    os.getenv("DATASETS_REFERENCE_FILEPATH", DATA_DIR / "datasets_reference.json"), "r"
) as f:
    TRACKED_DATASETS = json.load(f)

BOOKMARK_FILEPATH = Path(
    os.getenv("BOOKMARK_FILEPATH", DATA_DIR / "system" / "processed_bookmarks.json")
)

# Liste et ordre des colonnes pour le mono dataframe de base (avant normalisation et spécialisation)
# Sert aussi à vérifier qu'au moins ces colonnes sont présentes (d'autres peuvent être présentes en plus)
BASE_DF_COLUMNS = [
    "uid",
    "id",
    "nature",
    "acheteur_id",
    "acheteur_nom",
    "acheteur_siren",
    "titulaire_id",
    "titulaire_typeIdentifiant",
    "titulaire_nom",
    "titulaire_siren",
    "objet",
    "montant",
    "codeCPV",
    "procedure",
    "dureeMois",
    "dateNotification",
    "datePublicationDonnees",
    "formePrix",
    "attributionAvance",
    "offresRecues",
    "marcheInnovant",
    "ccag",
    "sousTraitanceDeclaree",
    "typeGroupementOperateurs",
    "tauxAvance",
    "origineUE",
    "origineFrance",
    "lieuExecution_code",
    "lieuExecution_typeCode",
    "idAccordCadre",
    "source",
    "sourceOpenData",
]

COLUMNS_TO_DROP = [
    # Pas encore incluses
    "typesPrix",
    "considerationsEnvironnementales",
    "considerationsSociales",
    "techniques",
    "modalitesExecution",
    "actesSousTraitance",
    "modificationsActesSousTraitance",
    # Champs de concessions
    "_type",  # Marché ou Contrat de concession
    "autoriteConcedante",
    "concessionnaires",
    "donneesExecution",
    "valeurGlobale",
    "montantSubventionPublique",
    "dateSignature",
    "dateDebutExecution",
    # Champs ajoutés par e-marchespublics (decp-2022)
    "offresRecues_source",
    "marcheInnovant_source",
    "attributionAvance_source",
    "sousTraitanceDeclaree_source",
    "dureeMois_source",
]

# Liste des ID de ressources présentes dans un dataset à traiter, mais exclues du traitement
EXCLUDED_RESOURCES = [
    "17046b18-8921-486a-bc31-c9196d5c3e9c"  # fichier XML consolidé par le MINEF mais abandonné
    "68bd2001-3420-4d94-bc49-c90878df322c"  # Fichier au format OCDS, pas DECP
]

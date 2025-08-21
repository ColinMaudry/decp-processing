import json
import os
import shutil
from datetime import datetime
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

dotenv_path = find_dotenv()
if dotenv_path == "":
    print("Création du fichier .env à partir de template.env")
    template_dotenv_path = Path(find_dotenv("template.env"))
    dotenv_path = template_dotenv_path.with_name(".env")
    shutil.copyfile(template_dotenv_path, dotenv_path)

load_dotenv(dotenv_path, override=False)

# Seuil de confiance pour la détection du format des données - en pratique c'est inutile car il n'y a qu'un format par fichier
FORMAT_DETECTION_QUORUM = 0.7

# Nombre maximal de workers utilisables par Prefect. Défaut : 16
MAX_PREFECT_WORKERS = int(os.getenv("MAX_PREFECT_WORKERS", 16))

# Durée avant l'expiration du cache des ressources (en heure). Défaut : 168 (7 jours)
CACHE_EXPIRATION_TIME_HOURS = int(os.getenv("CACHE_EXPIRATION_TIME_HOURS", 168))

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]  # YYYY-MM

# Publication ou non des fichiers produits sur data.gouv.fr
DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

# Dossier racine
BASE_DIR = Path(__file__).parent.parent

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(exist_ok=True, parents=True)

DIST_DIR = Path(os.getenv("DECP_DIST_DIR", BASE_DIR / "dist"))
DIST_DIR.mkdir(exist_ok=True, parents=True)

sirene_data_parent_dir = Path(os.getenv("SIRENE_DATA_PARENT_DIR", DATA_DIR))
SIRENE_DATA_DIR = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"
# SIRENE_DATA_DIR on ne le crée que si nécessaire, dans flows.py

# Dossier de stockage des résultats de tâches et du cache
# https://docs.prefect.io/v3/advanced/results#default-persistence-configuration
PREFECT_LOCAL_STORAGE_PATH = Path(os.getenv("PREFECT_LOCAL_STORAGE_PATH"))
PREFECT_LOCAL_STORAGE_PATH.mkdir(exist_ok=True, parents=True)

# POSTGRESQL
POSTGRESQL_DB_URI = os.getenv("POSTGRESQL_DB_URI")


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
    "modification_id",
    "donneesActuelles",
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
    # Inutilisée (on se base sur la date de la modification pour trier les modifications)
    "modification.id"
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
    "17046b18-8921-486a-bc31-c9196d5c3e9c",  # decp.xml : fichier XML consolidé par le MINEF mais abandonné
    "68bd2001-3420-4d94-bc49-c90878df322c",  # decp.ocds.json : fichier au format JSON mais OCDS, pas DECP
    "59ba0edb-cf94-4bf1-a546-61f561553917",  # decp-2022.json : format bizarre, entre 2019 et 2022 ~8000 marchés
    "16962018-5c31-4296-9454-5998585496d2",  # decp-2019.json : format DECP 2019, pas encore supporté
]

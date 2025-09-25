import json
import os
import shutil
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from ijson import sendable_list

from schemas import SCHEMA_MARCHE_2019, SCHEMA_MARCHE_2022

dotenv_path = find_dotenv()
if dotenv_path == "":
    print("Création du fichier .env à partir de template.env")
    template_dotenv_path = Path(find_dotenv("template.env"))
    dotenv_path = template_dotenv_path.with_name(".env")
    shutil.copyfile(template_dotenv_path, dotenv_path)

load_dotenv(dotenv_path, override=False)


def make_path_from_env(env: str, alternative_path: Path) -> Path:
    # J'ai eu des comportements erratiques avec os.getenv("env", alternative_path), donc j'utilise "or"
    return Path(os.getenv(env) or alternative_path)


# Nombre maximal de workers utilisables par Prefect. Défaut : 16
MAX_PREFECT_WORKERS = int(os.getenv("MAX_PREFECT_WORKERS", 16))

# Durée avant l'expiration du cache des ressources (en heure). Défaut : 168 (7 jours)
CACHE_EXPIRATION_TIME_HOURS = int(os.getenv("CACHE_EXPIRATION_TIME_HOURS", 168))

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]  # YYYY-MM

# Publication ou non des fichiers produits sur data.gouv.fr
DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

# Timeout pour la publication de chaque ressource sur data.gouv.fr
DECP_PROCESSING_PUBLISH_TIMEOUT = int(
    os.environ.get("DECP_PROCESSING_PUBLISH_TIMEOUT", 300)
)

# Clé d'API data.gouv.fr
API_KEY = os.environ.get("DATAGOUVFR_API_KEY", "")

# Dossier racine
BASE_DIR = Path(__file__).parent.parent

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = make_path_from_env("DATA_DIR", BASE_DIR / "data")
DATA_DIR.mkdir(exist_ok=True, parents=True)

DIST_DIR = make_path_from_env("DECP_DIST_DIR", BASE_DIR)
DIST_DIR.mkdir(exist_ok=True, parents=True)

sirene_data_parent_dir = make_path_from_env("SIRENE_DATA_PARENT_DIR", DATA_DIR)
SIRENE_DATA_DIR = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"
# SIRENE_DATA_DIR on ne le crée que si nécessaire, dans flows.py

# Dossier de stockage des résultats de tâches et du cache
# https://docs.prefect.io/v3/advanced/results#default-persistence-configuration
PREFECT_LOCAL_STORAGE_PATH = make_path_from_env(
    "PREFECT_LOCAL_STORAGE_PATH", DATA_DIR / "prefect_storage"
)

PREFECT_LOCAL_STORAGE_PATH.mkdir(exist_ok=True, parents=True)

# POSTGRESQL
POSTGRESQL_DB_URI = os.getenv("POSTGRESQL_DB_URI")


with open(
    make_path_from_env(
        "DATASETS_REFERENCE_FILEPATH", DATA_DIR / "source_datasets.json"
    ),
    "r",
) as f:
    TRACKED_DATASETS = json.load(f)

# Liste et ordre des colonnes pour le mono dataframe de base (avant normalisation et spécialisation)
# Sert aussi à vérifier qu'au moins ces colonnes sont présentes (d'autres peuvent être présentes en plus, les colonnes "innatendues")
BASE_DF_COLUMNS = [
    "uid",
    "id",
    "nature",
    "acheteur_id",
    "acheteur_nom",
    "titulaire_id",
    "titulaire_typeIdentifiant",
    "titulaire_nom",
    "objet",
    "montant",
    "codeCPV",
    "procedure",
    "techniques",
    "dureeMois",
    "offresRecues",
    "dateNotification",
    "datePublicationDonnees",
    "formePrix",
    "typesPrix",
    "attributionAvance",
    "tauxAvance",
    "marcheInnovant",
    "modalitesExecution",
    "considerationsSociales",
    "considerationsEnvironnementales",
    "ccag",
    "sousTraitanceDeclaree",
    "typeGroupementOperateurs",
    "origineUE",
    "origineFrance",
    "lieuExecution_code",
    "lieuExecution_typeCode",
    "idAccordCadre",
    "modification_id",
    "donneesActuelles",
    "sourceDataset",
    "sourceFile",
]

COLUMNS_TO_DROP = [
    # Pas encore incluses
    "actesSousTraitance",
    "modificationsActesSousTraitance",
    # Inutilisée (on se base sur la date de la modification pour trier les modifications)
    "modification_id"
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

# Liste des ID de ressources présentes dans un dataset à traiter, au format JSON ou XML, mais exclues du traitement
EXCLUDED_RESOURCES = [
    "17046b18-8921-486a-bc31-c9196d5c3e9c",  # decp.xml : fichier XML consolidé par le MINEF mais abandonné
    "68bd2001-3420-4d94-bc49-c90878df322c",  # decp.ocds.json : fichier au format JSON mais OCDS, pas DECP
    "59ba0edb-cf94-4bf1-a546-61f561553917",  # decp-2022.json : format bizarre, entre 2019 et 2022 ~8000 marchés
    "9c4f84d6-6fc9-4c82-a7f8-fb60d54fa188",  # données Région Bretagne inexistante (404)
    "8133b5b6-d097-4a5f-91cc-e94b11db3e2a",  # données Région Bretagne inexistante (404)
    "e10ea7c4-4992-45d8-a191-07dac6991f89",  # données Région Bretagne inexistante (404)
    "2d6dd1a6-8471-48c0-a207-6715cff06a99",  # données Région Bretagne JSON non-réglementaire
    "7629a6a1-3b8a-4570-8562-3a7cf82be88e",  # données Région Bretagne XML non-réglementaire
]


@dataclass
class DecpFormat:
    label: str
    schema: dict
    prefixe_json_marches: str
    liste_marches_ijson: sendable_list | None = None
    coroutine_ijson: Coroutine | None = None


DECP_FORMAT_2019 = DecpFormat("DECP 2019", SCHEMA_MARCHE_2019, "marches")
DECP_FORMAT_2022 = DecpFormat("DECP 2022", SCHEMA_MARCHE_2022, "marches.marche")
DECP_FORMATS = [DECP_FORMAT_2019, DECP_FORMAT_2022]

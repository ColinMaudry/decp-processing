import json
import os
import shutil
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from dotenv import find_dotenv, load_dotenv
from ijson import sendable_list

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


print("""
##########
# Config #
##########
""")


# Nombre maximal de workers utilisables par Prefect. Défaut : 16
MAX_PREFECT_WORKERS = int(os.getenv("MAX_PREFECT_WORKERS", 16))
print(f"{'MAX_PREFECT_WORKERS':<40}", MAX_PREFECT_WORKERS)

# Durée avant l'expiration du cache des ressources (en heure). Défaut : 168 (7 jours)
CACHE_EXPIRATION_TIME_HOURS = int(os.getenv("CACHE_EXPIRATION_TIME_HOURS", 168))
print(f"{'CACHE_EXPIRATION_TIME_HOURS':<40}", CACHE_EXPIRATION_TIME_HOURS)


DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]  # YYYY-MM

# Publication ou non des fichiers produits sur data.gouv.fr
DECP_PROCESSING_PUBLISH = os.getenv("DECP_PROCESSING_PUBLISH", "").lower() == "true"
print(f"{'DECP_PROCESSING_PUBLISH':<40}", DECP_PROCESSING_PUBLISH)


# Client HTTP
HTTP_CLIENT = httpx.Client()
HTTP_HEADERS = {
    "Connection": "keep-alive",
    "User-agent": "Projet : https://decp.info/a-propos | Client HTTP : https://pypi.org/project/httpx/",
}

# Timeout pour la publication de chaque ressource sur data.gouv.fr
DECP_PROCESSING_PUBLISH_TIMEOUT = os.getenv("DECP_PROCESSING_PUBLISH_TIMEOUT", 300)
if DECP_PROCESSING_PUBLISH_TIMEOUT == "":
    DECP_PROCESSING_PUBLISH_TIMEOUT = 300
else:
    DECP_PROCESSING_PUBLISH_TIMEOUT = int(DECP_PROCESSING_PUBLISH_TIMEOUT)

# URL de l'API data.gouv.fr
DATAGOUVFR_API = "https://www.data.gouv.fr/api/1"

# Clé d'API data.gouv.fr
DATAGOUVFR_API_KEY = os.getenv("DATAGOUVFR_API_KEY", "")

# Dossier racine
BASE_DIR = make_path_from_env("DECP_BASE_DIR", Path(__file__).absolute().parent.parent)
print(f"{'BASE_DIR':<40}", BASE_DIR)

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = make_path_from_env("DECP_DATA_DIR", BASE_DIR / "data")
DATA_DIR.mkdir(exist_ok=True, parents=True)
print(f"{'DATA_DIR':<40}", DATA_DIR)

DIST_DIR = make_path_from_env("DECP_DIST_DIR", BASE_DIR / "dist")
DIST_DIR.mkdir(exist_ok=True, parents=True, mode=777)
print(f"{'DIST_DIR':<40}", DIST_DIR)


def make_sirene_data_dir(sirene_data_parent_dir) -> Path:
    default_dir = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"
    # Si on est au début du mois, utiliser les données SIRENE du mois précédent
    # car les nouvelles données n'ont peut-être pas été encore générées
    if int(DATE_NOW[-2:]) <= 5:
        last_month = datetime.today() - timedelta(days=27)
        last_month = f"{str(last_month.year)}-{str(last_month.month)}"
        return sirene_data_parent_dir / f"sirene_{last_month}"
    return default_dir


SIRENE_DATA_PARENT_DIR = make_path_from_env("SIRENE_DATA_PARENT_DIR", DATA_DIR)
SIRENE_DATA_DIR = make_sirene_data_dir(SIRENE_DATA_PARENT_DIR)
# SIRENE_DATA_DIR on ne le crée que si nécessaire, dans flows.py
print(f"{'SIRENE_DATA_PARENT_DIR':<40}", SIRENE_DATA_PARENT_DIR)
print(f"{'SIRENE_DATA_DIR':<40}", SIRENE_DATA_DIR)


SIRENE_UNITES_LEGALES_URL = os.getenv("SIRENE_UNITES_LEGALES_URL", "")

# Mode de scraping
SCRAPING_MODE = os.getenv("SCRAPING_MODE", "month")
print(f"{'SCRAPING_MODE':<40}", SCRAPING_MODE)

# Target (plateforme cible pour le scraping)
SCRAPING_TARGET = os.getenv("SCRAPING_TARGET")
print(f"{'SCRAPING_TARGET':<40}", SCRAPING_TARGET)


# Dossier de stockage des résultats de tâches et du cache
# https://docs.prefect.io/v3/advanced/results#default-persistence-configuration
PREFECT_LOCAL_STORAGE_PATH = make_path_from_env(
    "PREFECT_LOCAL_STORAGE_PATH", DATA_DIR / "prefect_storage"
)
print(f"{'PREFECT_LOCAL_STORAGE_PATH':<40}", PREFECT_LOCAL_STORAGE_PATH)

PREFECT_LOCAL_STORAGE_PATH.mkdir(exist_ok=True, parents=True)

# POSTGRESQL
POSTGRESQL_DB_URI = os.getenv("POSTGRESQL_DB_URI")

# Liste et ordre des colonnes pour le mono dataframe de base (avant normalisation et spécialisation)
# Sert aussi à vérifier qu'au moins ces colonnes sont présentes (d'autres peuvent être présentes en plus, les colonnes "innatendues")
schema_fields = json.load(open(DATA_DIR / "schema_base.json", "r"))["fields"]
BASE_DF_COLUMNS = [field["name"] for field in schema_fields]

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
EXCLUDED_RESOURCES = os.getenv("EXCLUDED_RESOURCES", "").replace(" ", "")
print(f"{'EXCLUDED_RESOURCES':<40}", EXCLUDED_RESOURCES)

EXCLUDED_RESOURCES = EXCLUDED_RESOURCES.split(",")
EXCLUDED_RESOURCES = (
    [
        "17046b18-8921-486a-bc31-c9196d5c3e9c",  # decp.xml : fichier XML consolidé par le MINEF mais abandonné
        "68bd2001-3420-4d94-bc49-c90878df322c",  # decp.ocds.json : fichier au format JSON mais OCDS, pas DECP
        "59ba0edb-cf94-4bf1-a546-61f561553917",  # decp-2022.json : format bizarre, entre 2019 et 2022 ~8000 marchés
        "9c4f84d6-6fc9-4c82-a7f8-fb60d54fa188",  # données Région Bretagne inexistante (404)
        "8133b5b6-d097-4a5f-91cc-e94b11db3e2a",  # données Région Bretagne inexistante (404)
        "e10ea7c4-4992-45d8-a191-07dac6991f89",  # données Région Bretagne inexistante (404)
        "2d6dd1a6-8471-48c0-a207-6715cff06a99",  # données Région Bretagne JSON non-réglementaire
        "7629a6a1-3b8a-4570-8562-3a7cf82be88e",  # données Région Bretagne XML non-réglementaire
        "3bdd5a64-c28e-4c6a-84fd-5a28bcaa53e9",  # remplacements de chaînes de caractères dans les fichiers JSON AWS
    ]
    + EXCLUDED_RESOURCES
)

# Liste des datasets à traiter

# Ne traiter qu'un seul dataset identifier par son ID
SOLO_DATASET = os.getenv("SOLO_DATASET", "")
print(f"{'SOLO_DATASET':<40}", SOLO_DATASET)

with open(
    make_path_from_env(
        "DATASETS_REFERENCE_FILEPATH", DATA_DIR / "source_datasets.json"
    ),
    "r",
) as f:
    TRACKED_DATASETS = json.load(f)
for dataset in TRACKED_DATASETS:
    if dataset["id"] == SOLO_DATASET:
        TRACKED_DATASETS = [dataset]

# Ces marchés ont des montants invalides, donc on les met à 1 euro.
MARCHES_BAD_MONTANT = ["221300015002472020F00075"]


@dataclass
class DecpFormat:
    label: str
    schema: dict
    prefixe_json_marches: str
    liste_marches_ijson: sendable_list | None = None
    coroutine_ijson: Coroutine | None = None


print("")

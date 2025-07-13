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

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]

DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

BASE_DIR = Path(__file__).parent.parent

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(exist_ok=True)

DIST_DIR = Path(os.getenv("DECP_DIST_DIR", BASE_DIR / "dist"))
DIST_DIR.mkdir(exist_ok=True)

sirene_data_parent_dir = Path(os.getenv("SIRENE_DATA_PARENT_DIR", DATA_DIR))
SIRENE_DATA_DIR = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"

with open(os.getenv("DECP_JSON_FILES_PATH", DATA_DIR / "decp_json_files.json")) as f:
    DECP_JSON_FILES = json.load(f)

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

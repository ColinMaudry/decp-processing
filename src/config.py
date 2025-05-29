from datetime import datetime
from dotenv import load_dotenv
import json
import os
import shutil

if not os.path.exists(".env"):
    print("Création du fichier .env à partir de template.env")
    shutil.copyfile("template.env", ".env")

if not os.path.exists("./dist"):
    os.mkdir("dist")

# Les variables configurées sur le serveur doivent avoir la priorité
load_dotenv(override=False)

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[2:10]

DIST_DIR = f"dist/" + DATE_NOW
DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

SIRENE_DATA_DIR = os.getenv("SIRENE_DATA_DIR")

with open(os.getenv("DECP_JSON_FILES_PATH", "data/decp_json_files.json")) as f:
    DECP_JSON_FILES = json.load(f)

# Liste et ordre des colonnes pour le mono dataframe de base (avant normalisation et spécialisation)
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
    "source_open_data",
]

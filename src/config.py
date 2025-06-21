import json
import os
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
from dotenv import find_dotenv, load_dotenv

dotenv_path = find_dotenv()
if dotenv_path == "":
    print("Création du fichier .env à partir de template.env")
    template_dotenv_path = Path(find_dotenv("template.env"))
    dotenv_path = template_dotenv_path.with_name(".env")
    shutil.copyfile(template_dotenv_path, dotenv_path)

load_dotenv(dotenv_path, override=False)

DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[2:10]

DECP_PROCESSING_PUBLISH = os.environ.get("DECP_PROCESSING_PUBLISH", "")

BASE_DIR = Path(__file__).parent.parent

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(exist_ok=True)

DIST_DIR = Path(os.getenv("DECP_DIST_DIR", BASE_DIR / "dist"))
DIST_DIR.mkdir(exist_ok=True)

SIRENE_DATA_DIR = Path(os.getenv("SIRENE_DATA_DIR", DATA_DIR / "sirene"))
SIRENE_DATA_DIR.mkdir(exist_ok=True)

with open(os.getenv("DECP_JSON_FILES_PATH", DATA_DIR / "decp_json_files.json")) as f:
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
    "sourceOpenData",
]

TITULAIRE_SCHEMA = pl.Struct(
    {
        "typeIdentifiant": pl.String,
        "id": pl.String,
    }
)

CONCESSIONS_SCHEMA = {
    "autoriteConcedante.id": pl.String,
    "valeurGlobale": pl.String,
    "modification.valeurGlobale": pl.String,
}

FLAT_MOD_SCHEMA = {
    "modification.id": pl.Int32,  # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
    "modification.objetModification": pl.String,
    "modification.dateNotificationModification": pl.String,
    "modification.datePublicationDonneesModification": pl.String,
    "modification.typeIdentifiant": pl.String,
    "modification.montant": pl.String,
    "modification.dateSignatureModification": pl.String,
    "modification.dureeMois": pl.String,
    "modification.titulaires.typeIdentifiant": pl.String,
    "modification.titulaires.id": pl.String,
}

BASE_MARCHE_SCHEMA = {
    "procedure": pl.String,
    "nature": pl.String,
    "codeCPV": pl.String,
    "dureeMois": pl.String,
    "datePublicationDonnees": pl.String,
    "titulaires": pl.List(TITULAIRE_SCHEMA),
    "id": pl.String,
    "formePrix": pl.String,
    "dateNotification": pl.String,
    "objet": pl.String,
    "montant": pl.String,
    "acheteur.id": pl.String,
    "source": pl.String,
    "lieuExecution.code": pl.String,
    "lieuExecution.typeCode": pl.String,
    "_type": pl.String,
    "uid": pl.String,
    "uuid": pl.String,
    "considerationsSociales": pl.List(pl.String),
    "considerationsEnvironnementales": pl.List(pl.String),
    # "modaliteExecution": pl.List(pl.String),
    "marcheInnovant": pl.Boolean,
    "attributionAvance": pl.Boolean,
    "sousTraitanceDeclaree": pl.Boolean,
    "ccag": pl.String,
    "offresRecues": pl.Float64,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    # "technique": pl.List(pl.String),
    "TypePrix": pl.String,
    "tauxAvance": pl.Float64,
    "origineUE": pl.Float64,
    "origineFrance": pl.Float64,
    "created_at": pl.String,
    "typesPrix": pl.List(pl.String),
    "typePrix": pl.String,
    "term.acheteur.id": pl.String,
    "updated_at": pl.String,
}

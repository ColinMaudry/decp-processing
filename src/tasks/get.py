import json
import os
import re
from pathlib import Path

import ijson
import orjson
import polars as pl
import xmltodict
from httpx import get
from polars.polars import ColumnNotFoundError
from prefect import task

import tasks.bookmarking as bookmarking
from config import (
    DATA_DIR,
    DATE_NOW,
    DIST_DIR,
    FORMAT_DETECTION_QUORUM,
    TRACKED_DATASETS,
)
from schemas import MARCHE_SCHEMA_2022
from tasks.clean import load_and_fix_json
from tasks.dataset_utils import list_resources_to_process
from tasks.detect_format import detect_format
from tasks.output import save_to_files
from tasks.setup import create_table_artifact


def clean_xml(xml_str):
    # Remove invalid XML 1.0 characters
    return re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", xml_str)


def parse_xml(url: str) -> None:
    """
    Fonction utilitaire pour parser un fichier XML en JSON.
    Utilisée pour convertir les fichiers XML des DECP en JSON.
    """
    # Cette fonction est un placeholder pour le moment.
    # Elle peut être développée si nécessaire.

    request = get(url, follow_redirects=True)
    data = xmltodict.parse(
        clean_xml(request.text),
        force_list={
            "considerationSociale",
            "considerationEnvironnementale",
            "modaliteExecution",
            "technique",
            "typePrix",
            "modification",
            "titulaire",
        },
    )

    if "marches" in data:
        if "marche" in data["marches"]:
            # modifications des titulaires et des modifications
            for row in data["marches"]["marche"]:
                mods = row.get("modifications", {})
                if "modification" in mods:
                    mods_list = mods["modification"]
                    row["modifications"] = [{"modification": m} for m in mods_list]

                titulaires = row.get("titulaires", {})
                if "titulaire" in titulaires:
                    titulaires_list = titulaires["titulaire"]
                    row["titulaires"] = [{"titulaire": t} for t in titulaires_list]

            for row in data["marches"]["marche"]:
                mods = row.get("modifications", [])
                _mods = []
                for mod in mods:
                    if "titulaires" in mod["modification"]:
                        mod["modification"].update(
                            {"titulaires": [mod["modification"]["titulaires"]]}
                        )
                    _mods.append(mod)

                if _mods:
                    row["modifications"] = _mods

    return data


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, file_info: dict):
    url = file_info["url"]
    filename = file_info["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = DATA_DIR / f"{filename}_{date_now}.json"
        if not (os.path.exists(decp_json_file)):
            if file_info["file_format"] == "json":
                request = get(url, follow_redirects=True)
                with open(decp_json_file, "wb") as file:
                    file.write(request.content)
            elif file_info["file_format"] == "xml":
                # Convert XML to JSON
                data = parse_xml(url)
                with open(decp_json_file, "w", encoding="utf-8") as file:
                    json.dump(data, file, ensure_ascii=False, indent=2)
        else:
            print(f"[{filename}] DECP d'aujourd'hui déjà téléchargées ({date_now})")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(url)

    return decp_json_file


@task(retries=5, retry_delay_seconds=5)
def get_json_metadata(dataset_id: str, resource_id: str) -> dict:
    """Téléchargement des métadonnées d'une ressoure (fichier)."""
    api_url = (
        f"http://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    )
    json_metadata = get(api_url, follow_redirects=True).json()
    return json_metadata


@task
def get_decp_json() -> list[Path]:
    """Téléchargement des DECP publiées par Bercy sur data.gouv.fr."""

    date_now = DATE_NOW

    # Recuperation des fichiers à traiter
    json_files = list_resources_to_process(TRACKED_DATASETS)

    return_files = []
    downloaded_files = []
    artefact = []

    for json_file in json_files:
        artifact_row = {}
        # Telechargement du fichier JSON
        decp_json_file: Path = get_json(date_now, json_file)

        # Chargement du fichier JSON
        with open(decp_json_file, encoding="utf8") as f:
            decp_json = json.load(f)

        # Determiner le format du fichier
        if json_file["file_name"] == "5cd57bf68b4c4179299eb0e9-decp-2022":
            format_decp = "2022-messy"  # pour ce fichier en particulier qui comporte des erreurs, on bypass pour le moment
        else:
            format_decp = detect_format(
                decp_json, FORMAT_DETECTION_QUORUM
            )  #'empty', '2019' ou '2022'

        if format_decp == "2022":
            dataset_name = {
                d["dataset_id"]: d["dataset_name"] for d in TRACKED_DATASETS
            }.get(json_file["dataset_id"])

            print(
                f"JSON -> DF - format 2022: {json_file['file_name']} ({dataset_name})"
            )

            if json_file["url"].startswith("https"):
                decp_json_metadata = get_json_metadata(
                    dataset_id=json_file["dataset_id"],
                    resource_id=json_file["resource_id"],
                )
                artifact_row = {
                    "open_data_dataset_id": json_file["dataset_id"],
                    "open_data_dataset_name": dataset_name,
                    "open_data_filename": decp_json_metadata["title"],
                    "open_data_id": decp_json_metadata["id"],
                    "sha1": decp_json_metadata["checksum"]["value"],
                    "created_at": decp_json_metadata["created_at"],
                    "last_modified": decp_json_metadata["last_modified"],
                    "filesize": decp_json_metadata["filesize"],
                    "views": decp_json_metadata["metrics"]["views"],
                }

            filename = json_file["file_name"]

            df: pl.DataFrame = json_to_df(decp_json_file)

            artifact_row["open_data_dataset_id"] = json_file["dataset_id"]
            artifact_row["download_date"] = date_now
            artifact_row["columns"] = sorted(df.columns)
            artifact_row["column_number"] = len(df.columns)
            artifact_row["row_number"] = df.height

            artefact.append(artifact_row)

            df = df.with_columns(
                pl.lit(f"data.gouv.fr {filename}.json").alias("sourceOpenData")
            )

            # Pour l'instant on ne garde pas les champs qui demandent une explosion
            # ou une eval à part:
            # - titulaires
            # - modifications

            columns_to_drop = [
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

            absent_columns = []
            for col in columns_to_drop:
                try:
                    df = df.drop(col)
                except ColumnNotFoundError:
                    absent_columns.append(col)
                    pass

            print(f"[{filename}]", df.shape)

            file_path = DIST_DIR / "get" / f"{filename}_{date_now}"
            file_path.parent.mkdir(exist_ok=True)
            save_to_files(df, file_path, ["parquet"])

            return_files.append(file_path)
            downloaded_files.append(filename + ".json")

            # si le dataset est incrémental, on bookmark la ressource traitée
            if {
                dataset["dataset_id"]: dataset["incremental"]
                for dataset in TRACKED_DATASETS
            }.get(json_file["dataset_id"], False):
                bookmarking.bookmark(json_file["resource_id"])

    # Stock les statistiques dans prefect
    create_table_artifact(
        table=artefact,
        key="datagouvfr-json-resources",
        description=f"Les ressources JSON des DECP consolidées au format JSON ({date_now})",
    )
    # Stocke la liste des fichiers pour la réutiliser plus tard pour la création d'un artefact
    os.environ["downloaded_files"] = ",".join(downloaded_files)

    return return_files


def json_to_df(json_path_file) -> pl.DataFrame:
    ndjson_path = json_path_file.with_suffix(".ndjson")
    json_to_ndjson(json_path_file, ndjson_path)
    schema = MARCHE_SCHEMA_2022
    dff = pl.read_ndjson(ndjson_path, schema=schema)
    return dff


def json_to_ndjson(json_path: Path, ndjson_path: Path):
    with open(json_path, "rb") as _in_f:
        with open(ndjson_path, "wb") as out_f:
            _data = load_and_fix_json(_in_f)
            marches = ijson.items(_data, "item", use_float=True)
            for marche in marches:
                # Aplatissement de acheteur et lieuExecution (acheteur_id, etc.)
                marche = pl.convert.normalize._simple_json_normalize(
                    marche, "_", 10, lambda x: x
                )
                marche = orjson.dumps(marche)
                out_f.write(marche)
                out_f.write(b"\n")

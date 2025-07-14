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

from config import (
    COLUMNS_TO_DROP,
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
    # Suppresion des caractères invalides XML 1.0
    return re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", xml_str)


def parse_xml(url: str) -> dict:
    """
    Récupère et parse un fichier XML à partir d'une URL, puis le convertit en JSON.

    Cette fonction est conçue pour traiter les fichiers XML issus des DECP. Elle applique
    un post-traitement sur les nœuds "modifications" et "titulaires" pour uniformiser leur structure.

    Paramètre :
        url (str) : L'URL du fichier XML à récupérer et à parser.

    Retour :
        dict : Une représentation sous forme de dictionnaire du contenu XML, avec une structure adaptée pour un usage en JSON.
    """

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
            "marche",
        },
    )

    if "marches" in data:
        if "marche" in data["marches"]:
            # modifications des titulaires et des modifications
            for row in data["marches"]["marche"]:
                mods = row.get("modifications", [])
                if mods is None:
                    row["modifications"] = []
                elif "modification" in mods:
                    mods_list = mods["modification"]
                    row["modifications"] = [{"modification": m} for m in mods_list]

                titulaires = row.get("titulaires", [])
                if titulaires is None:
                    row["titulaires"] = []
                elif "titulaire" in titulaires:
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
                # Conversion du XML en JSON
                data = parse_xml(url)
                with open(decp_json_file, "w", encoding="utf-8") as file:
                    json.dump(data, file, ensure_ascii=False, indent=2)
        else:
            print(f"☑️  DECP déjà téléchargées pour aujourd'hui ({date_now})")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(url)

    return decp_json_file


@task(retries=5, retry_delay_seconds=2)
def get_json_metadata(dataset_id: str, resource_id: str) -> dict:
    """Téléchargement des métadonnées d'une ressoure (fichier)."""
    api_url = (
        f"http://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    )
    json_metadata = get(api_url, follow_redirects=True).json()
    return json_metadata


@task
def get_decp_json(dataset_id: str = None) -> list[Path]:
    """
    Téléchargement des DECP publiées par Bercy sur data.gouv.fr.

    On peut passer un `dataset_id` optionel pour restreindre l'extraction au dataset_id indiqué

    """

    date_now = DATE_NOW

    if dataset_id:
        datasets = [t for t in TRACKED_DATASETS if t["dataset_id"] == dataset_id]
    else:
        datasets = TRACKED_DATASETS

    # Récuperation des fichiers à traiter
    json_files = list_resources_to_process(datasets)
    json_files_nb = len(json_files)

    with open(DIST_DIR / "json_files.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(json_files, ensure_ascii=False, indent=2))

    return_files = []
    downloaded_files = []
    artefact = []

    for i, json_file in enumerate(json_files):
        if json_file["file_name"].startswith("5cd57bf68b4c4179299eb0e9_decp-2022"):
            continue  # pour ce fichier en particulier qui comporte des erreurs, on bypass pour le moment

        dataset_name = {
            d["dataset_id"]: d["dataset_name"] for d in TRACKED_DATASETS
        }.get(json_file["dataset_id"])
        print(
            f"➡️  {json_file['ori_file_name']} ({dataset_name}) -- {i}/{json_files_nb}"
        )
        print("Fichier : ", json_file["file_name"] + ".json")

        artifact_row = {}

        # Telechargement du fichier JSON
        decp_json_file: Path = get_json(date_now, json_file)

        # Chargement du fichier JSON
        with open(decp_json_file, encoding="utf8") as f:
            decp_json = json.load(f)

        # Determiner le format du fichier
        format_decp = detect_format(
            decp_json, FORMAT_DETECTION_QUORUM
        )  #'empty', '2019' ou '2022'

        if format_decp == "2022":
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
                    "views": decp_json_metadata["metrics"].get("views", None),
                }

            filename = json_file["file_name"]

            print("JSON -> DF (format 2022)...")
            df: pl.DataFrame = json_to_df(decp_json_file)

            artifact_row["open_data_dataset_id"] = json_file["dataset_id"]
            artifact_row["open_data_dataset_name"] = dataset_name
            artifact_row["download_date"] = date_now
            artifact_row["columns"] = sorted(df.columns)
            artifact_row["column_number"] = len(df.columns)
            artifact_row["row_number"] = df.height

            artefact.append(artifact_row)

            df = df.with_columns(
                pl.lit(f"data.gouv.fr {filename}.json").alias("sourceOpenData"),
                # Ajout d'une colonne datagouv_dataset_id
                pl.lit(json_file["dataset_id"]).alias("datagouv_dataset_id"),
            )

            absent_columns = []
            for col in COLUMNS_TO_DROP:
                try:
                    df = df.drop(col)
                except ColumnNotFoundError:
                    absent_columns.append(col)
                    pass

            print(df.shape)

            file_path = DIST_DIR / "get" / f"{filename}_{date_now}"
            file_path.parent.mkdir(exist_ok=True)
            save_to_files(df, file_path, ["parquet"])

            return_files.append(file_path)
            downloaded_files.append(filename + ".json")
        else:
            print(f"🙈  Le format {format_decp} n'est pas pris en charge.")

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

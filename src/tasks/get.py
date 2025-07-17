import io
import json
import re

import orjson
import polars as pl
import xmltodict
from httpx import get
from polars.polars import ColumnNotFoundError
from prefect import task

from config import (
    COLUMNS_TO_DROP,
    DATE_NOW,
    FORMAT_DETECTION_QUORUM,
)
from schemas import MARCHE_SCHEMA_2022
from tasks.clean import load_and_fix_json
from tasks.detect_format import detect_format


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


@task(retries=3, retry_delay_seconds=3)
def get_json(file_info: dict) -> dict:
    url = file_info["url"]
    json_content = {}

    if url.startswith("https"):
        # Prod file
        if file_info["file_format"] == "json":
            request = get(url, follow_redirects=True)
            json_content = json.loads(request.text)
        elif file_info["file_format"] == "xml":
            # Conversion du XML en JSON
            json_content = parse_xml(url)
        return json_content
    else:
        # Test file, pas de téléchargement
        f = open(url, "rb")
        return json.load(f)


@task
def get_resource(r: dict) -> pl.LazyFrame:
    """Téléchargement de la ressource."""

    date_now = DATE_NOW

    artefact = []
    artifact_row = {}

    # Téléchargement du fichier JSON
    decp_json = get_json(r)

    # Déterminer le format du fichier
    format_decp = detect_format(
        decp_json, FORMAT_DETECTION_QUORUM
    )  # 'empty', '2019' ou '2022'

    if format_decp == "2022":
        print(f"➡️  {r['ori_filename']} ({r['dataset_name']})")
        if r["url"].startswith("https"):
            artifact_row = {
                "open_data_dataset_id": r["dataset_id"],
                "open_data_dataset_name": r["dataset_name"],
                "open_data_filename": r["ori_filename"],
                "open_data_id": r["id"],
                "sha1": r["checksum"],
                "created_at": r["created_at"],
                "last_modified": r["last_modified"],
                "filesize": r["filesize"],
                "views": r["view"],
            }

        print("JSON -> DF (format 2022)...")
        df: pl.DataFrame = json_to_df(decp_json)

        artifact_row["open_data_dataset_id"] = r["dataset_id"]
        artifact_row["open_data_dataset_name"] = r["dataset_name"]
        artifact_row["download_date"] = date_now
        artifact_row["columns"] = sorted(df.columns)
        artifact_row["column_number"] = len(df.columns)
        artifact_row["row_number"] = df.height

        artefact.append(artifact_row)

        lf = df.lazy()
        resource_web_url = f"https://www.data.gouv.fr/fr/datasets/{r['dataset_id']}/#/resources/{r['id']}"
        lf = lf.with_columns(pl.lit(resource_web_url).alias("sourceOpenData"))

        absent_columns = []
        for col in COLUMNS_TO_DROP:
            try:
                df = df.drop(col)
            except ColumnNotFoundError:
                absent_columns.append(col)
                pass

    else:
        lf = pl.LazyFrame()

    return lf


def json_to_df(decp_json) -> pl.DataFrame:
    ndjson_filelike = json_to_ndjson(decp_json)
    schema = MARCHE_SCHEMA_2022
    dff = pl.read_ndjson(ndjson_filelike, schema=schema)
    return dff


def json_to_ndjson(decp_json: dict) -> io.BytesIO:
    _data = load_and_fix_json(decp_json)
    print(_data)

    # Pour l'instant plus de streaming en attendant decp-2019
    # marches = ijson.items(_data, "item", use_float=True)
    marches: list = _data

    buffer = io.BytesIO()
    for marche in marches:
        # Aplatissement de acheteur et lieuExecution (acheteur_id, etc.)
        marche = pl.convert.normalize._simple_json_normalize(
            marche, "_", 10, lambda x: x
        )
        buffer.write(orjson.dumps(marche))
        buffer.write(b"\n")
    return buffer

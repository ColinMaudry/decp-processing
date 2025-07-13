import os
from pathlib import Path

import ijson
import orjson
import polars as pl
from httpx import get
from polars.polars import ColumnNotFoundError
from prefect import task

from config import DATA_DIR, DATE_NOW, DECP_JSON_FILES
from schemas import MARCHE_SCHEMA_2022
from tasks.clean import load_and_fix_json
from tasks.output import save_to_files
from tasks.setup import create_table_artifact


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, json_file: dict):
    url = json_file["url"]
    filename = json_file["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = DATA_DIR / "get" / f"{filename}_{date_now}.json"
        decp_json_file.parent.mkdir(exist_ok=True)
        if not (os.path.exists(decp_json_file)):
            request = get(url, follow_redirects=True)
            with open(decp_json_file, "wb") as file:
                file.write(request.content)
        else:
            print(f"[{filename}] DECP d'aujourd'hui déjà téléchargées ({date_now})")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(url)

    return decp_json_file


@task(retries=5, retry_delay_seconds=5)
def get_json_metadata(json_file: dict) -> dict:
    """Téléchargement des métadonnées d'une ressoure (fichier)."""
    resource_id = json_file["url"].split("/")[-1]
    api_url = f"http://www.data.gouv.fr/api/1/datasets/5cd57bf68b4c4179299eb0e9/resources/{resource_id}/"
    json_metadata = get(api_url, follow_redirects=True).json()
    return json_metadata


@task
def get_decp_json() -> list[Path]:
    """Téléchargement des DECP publiées par Bercy sur data.gouv.fr."""

    json_files = DECP_JSON_FILES
    date_now = DATE_NOW

    return_files = []
    downloaded_files = []
    artefact = []

    for json_file in json_files:
        artifact_row = {}
        if json_file["process"] is True:
            decp_json_file: Path = get_json(date_now, json_file)

            if json_file["url"].startswith("https"):
                decp_json_metadata = get_json_metadata(json_file)
                artifact_row = {
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

            artifact_row["open_data_dataset"] = "data.gouv.fr JSON"
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

            file_path = DATA_DIR / "get" / f"{filename}_{date_now}"
            save_to_files(df, file_path, ["parquet"])

            return_files.append(file_path)
            downloaded_files.append(filename + ".json")

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

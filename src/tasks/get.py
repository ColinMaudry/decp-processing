import json
import os
from pathlib import Path

import ijson
import orjson
import polars as pl
from httpx import get, stream
from polars.polars import ColumnNotFoundError
from prefect import task

from config import (
    BASE_MARCHE_SCHEMA,
    DATA_DIR,
    DATE_NOW,
    DECP_JSON_FILES,
    DIST_DIR,
    FLAT_MOD_SCHEMA,
)
from tasks.output import save_to_files, sink_to_files
from tasks.setup import create_table_artifact


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, json_file: dict):
    url = json_file["url"]
    filename = json_file["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = DATA_DIR / f"{filename}_{date_now}.json"
        if not (os.path.exists(decp_json_file)):
            with stream("GET", url, follow_redirects=True) as response:
                with open(decp_json_file, "wb") as file:
                    for data in response.iter_bytes(1024**2):
                        file.write(data)
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
            downloaded_files.append(decp_json_file)

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

            artifact_row["open_data_dataset"] = "data.gouv.fr JSON"
            artifact_row["download_date"] = date_now

            filename = json_file["file_name"]
            if filename.endswith("2019"):
                df = json_to_df_2019(decp_json_file, artifact_row)
            else:
                df = json_to_df_2022(decp_json_file, artifact_row)

            artefact.append(artifact_row)
            print(
                f"[{decp_json_file}]",
                (artifact_row["row_number"], artifact_row["column_number"]),
            )

            file_path = DIST_DIR / "get" / f"{filename}_{date_now}"
            return_files.append(file_path)
            file_path.parent.mkdir(exist_ok=True)
            if isinstance(df, pl.DataFrame):
                save_to_files(df, file_path, ["parquet"])
            elif isinstance(df, pl.LazyFrame):
                sink_to_files(df, file_path, ["parquet"])

    # Stock les statistiques dans prefect
    create_table_artifact(
        table=artefact,
        key="datagouvfr-json-resources",
        description=f"Les ressources JSON des DECP consolidées au format JSON ({date_now})",
    )
    # Stocke la liste des fichiers pour la réutiliser plus tard pour la création d'un artefact
    os.environ["downloaded_files"] = ",".join(downloaded_files)

    return return_files


@task
def json_to_df_2022(input_file, artifact_row):
    with open(input_file, encoding="utf8") as f:
        decp_json = json.load(f)

    path = decp_json["marches"]["marche"]
    df: pl.DataFrame = pl.json_normalize(
        path,
        strict=False,
        # Pas de détection des dtypes, tout est pl.String pour commencer.
        infer_schema_length=10000,
        # encoder="utf8",
        # Remplacement des "." dans les noms de colonnes par des "_" car
        # en SQL ça oblige à entourer les noms de colonnes de guillemets
        separator="_",
    )

    artifact_row["columns"] = sorted(df.columns)
    artifact_row["column_number"] = len(df.columns)
    artifact_row["row_number"] = df.height

    df = df.with_columns(
        pl.lit(f"data.gouv.fr {input_file.name}").alias("sourceOpenData")
    )

    # Pour l'instant on ne garde pas les champs qui demandent une explosion
    # ou une eval à part titulaires

    columns_to_drop = [
        # Pas encore incluses
        "typesPrix_typePrix",
        "considerationsEnvironnementales_considerationEnvironnementale",
        "considerationsSociales_considerationSociale",
        "techniques_technique",
        "modalitesExecution_modaliteExecution",
        "modifications",
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

    return df


@task
def json_to_df_2019(json_path, artifact_row):
    ndjson_path = json_path.with_suffix(".ndjson")
    json_to_ndjson(json_path, ndjson_path, marches_path="marches")
    schema = {**BASE_MARCHE_SCHEMA, **FLAT_MOD_SCHEMA}
    df = pl.scan_ndjson(ndjson_path, schema=schema)
    artifact_row["columns"] = sorted(schema.keys())
    artifact_row["column_number"] = len(schema)
    artifact_row["row_number"] = df.select(pl.len()).collect().item()
    return df


@task
def json_to_ndjson(json_path, ndjson_path, marches_path="marches"):
    with open(json_path, "rb") as in_f:
        with open(ndjson_path, "wb") as out_f:
            marches = ijson.items(in_f, f"{marches_path}.item", use_float=True)
            for m in marches:
                for m in yield_mods(m):
                    out_f.write(orjson.dumps(m))
                    out_f.write(b"\n")


@task
def yield_mods(row: dict, separator="."):
    mods = [{}] + row.pop("modifications", [])
    for i, m in enumerate(mods):
        m["id"] = i
        row["modification"] = m
        yield pl.convert.normalize._simple_json_normalize(
            row, separator, 10, lambda x: x
        )

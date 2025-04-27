import polars as pl
from httpx import get
import os
import json

from polars.polars import ColumnNotFoundError
from prefect import task
from pathlib import Path

from tasks.output import save_to_files
from tasks.setup import create_table_artifact
from config import DIST_DIR, DECP_JSON_FILES, DATE_NOW


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, json_file: dict):
    url = json_file["url"]
    filename = json_file["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = Path(f"data/{filename}_{date_now}.json")
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
def get_decp_json() -> list:
    """Téléchargement des DECP publiées par Bercy sur data.gouv.fr."""

    json_files = DECP_JSON_FILES
    date_now = DATE_NOW

    return_files = []
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

            with open(decp_json_file, encoding="utf8") as f:
                decp_json = json.load(f)

            filename = json_file["file_name"]
            path = decp_json["marches"]["marche"]
            df: pl.DataFrame = pl.json_normalize(
                path,
                strict=False,
                infer_schema_length=10000,
                encoder="utf8",
                separator="_",
                # Remplacement des "." dans les noms de colonnes par des "_" car
                # en SQL ça oblige à entourer les noms de colonnes de guillemets
            )

            artifact_row["open_data_dataset"] = "data.gouv.fr JSON"
            artifact_row["download_date"] = date_now
            artifact_row["column_number"] = len(df.columns)
            artifact_row["row_number"] = df.height

            artefact.append(artifact_row)

            df = df.with_columns(
                pl.lit(f"data.gouv.fr {filename}.json").alias("source_open_data")
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
                # "modifications",
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

            if len(absent_columns) > 0:
                print(f"{filename}: colonnes à supprimer absentes : {absent_columns}")
            print(f"[{filename}]", df.shape)

            file = f"{DIST_DIR}/get/{filename}_{date_now}"
            if not os.path.exists(f"{DIST_DIR}/get"):
                os.mkdir(f"{DIST_DIR}/get")
            save_to_files(df, file, ["parquet"])

            return_files.append(file)

    # Stock les statistiques dans prefect cloud
    create_table_artifact(
        table=artefact,
        key="datagouvfr-json-resources",
        description=f"Les ressources JSON des DECP consolidées au format JSON ({date_now})",
    )
    return return_files


def get_stats():
    url = "https://www.data.gouv.fr/fr/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234"
    df_stats = pl.read_csv(url, index_col=None)
    return df_stats

import json
import os
from pathlib import Path

import polars as pl
from httpx import get
from polars.polars import ColumnNotFoundError
from prefect import task

from config import DATE_NOW, DECP_JSON_FILES, DIST_DIR
from tasks.output import save_to_files
from tasks.setup import create_table_artifact


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


def clean_json(input_json_):
    """
    Nettoyage des données JSON des DECP pour les modifications des titulaires.
    Suppression des données qui ne correspondent pas au format attendu (ex: {"typeIdentifiant": "SIRET", "id": "12345678901234"}).
    """
    clean_json = []
    titulaires_cleaned_cpt = 0
    for entry in input_json_:
        # entry = {} représentant un marché
        modifications_entries = entry.get("modifications", [])
        # modifications_entries = [] représentant les modifications du marché
        clean_modifications_entries = []
        for modification_entry in modifications_entries:
            # modification_entry = {} représentant une modification du marché
            modification_entry_clean = modification_entry["modification"]
            if "titulaires" in modification_entry_clean.keys():
                modification_titulaires_clean = []
                for modification_titulaire in modification_entry_clean.get(
                    "titulaires", []
                ):
                    # mofification_titulaire = {} représentant un titulaire de la modification
                    if isinstance(modification_titulaire["titulaire"], dict):
                        # Si le titulaire est un dictionnaire, on récupère l'id et le typeIdentifiant
                        modification_titulaires_clean.append(
                            {
                                "titulaire": {
                                    "typeIdentifiant": modification_titulaire[
                                        "titulaire"
                                    ].get("typeIdentifiant"),
                                    "id": modification_titulaire["titulaire"].get("id"),
                                }
                            }
                        )
                if modification_titulaires_clean:
                    modification_entry_clean["titulaires"] = (
                        modification_titulaires_clean
                    )
                else:
                    modification_entry_clean.pop("titulaires", None)
                    titulaires_cleaned_cpt += 1
            clean_modifications_entries.append(
                {"modification": modification_entry_clean}
            )
        entry["modifications"] = clean_modifications_entries
        clean_json.append(entry)
    print(f"Nombre de titulaires nettoyés : {titulaires_cleaned_cpt}")
    return clean_json


@task
def get_decp_json() -> list:
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

            with open(decp_json_file, encoding="utf8") as f:
                decp_json = json.load(f)

            filename = json_file["file_name"]
            path = decp_json["marches"]["marche"]

            # Nettoyage et simplification des données JSON
            path = clean_json(path)

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
            # ou une eval à part titulaires

            columns_to_drop = [
                # Pas encore incluses
                "typesPrix_typePrix",
                "considerationsEnvironnementales_considerationEnvironnementale",
                "considerationsSociales_considerationSociale",
                "techniques_technique",
                "modalitesExecution_modaliteExecution",
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

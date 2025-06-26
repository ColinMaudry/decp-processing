import json
import os
from pathlib import Path

import ijson
import orjson
import polars as pl
from httpx import get
from polars.polars import ColumnNotFoundError
from prefect import task

from config import (
    DATA_DIR,
    DATE_NOW,
    DIST_DIR,
    FORMAT_DETECTION_QUORUM,
    TRACKED_DATASETS,
)
from schemas import MARCHE_SCHEMA_2022
from tasks.clean import load_and_fix_json
from tasks.detect_format import detect_format
from tasks.output import save_to_files
from tasks.setup import create_table_artifact


@task(retries=5, retry_delay_seconds=5)
def get_json(date_now, json_file: dict):
    url = json_file["url"]
    filename = json_file["file_name"]

    if url.startswith("https"):
        # Prod file
        decp_json_file: Path = DATA_DIR / f"{filename}_{date_now}.json"
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
    json_files = list_resources_to_process([d["dataset_id"] for d in TRACKED_DATASETS])

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
            format = "2022-messy"  # pour ce fichier en particulier qui comporte des erreurs, on bypass pour le moment
        else:
            format = detect_format(
                decp_json, FORMAT_DETECTION_QUORUM
            )  #'empty', '2019' ou '2022'

        if format == "2022":
            if json_file["url"].startswith("https"):
                decp_json_metadata = get_json_metadata(
                    dataset_id=json_file["dataset_id"],
                    resource_id=json_file["resource_id"],
                )
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

            file_path = DIST_DIR / "get" / f"{filename}_{date_now}"
            file_path.parent.mkdir(exist_ok=True)
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


def list_datasets_by_org(org_id: str) -> list[dict]:
    """
    Liste tous les jeux de données (datasets) produits par une organisation.

    Utile, par exemple, pour détecter tous les jeux de données publiés par une
    organisation spécifique comme Atexo.

    Voir la documentation de l'API :
    https://guides.data.gouv.fr/guide-data.gouv.fr/readme-1/reference/organizations#get-organizations-org-datasets

    Parameters
    ----------
    org_id : str
        Identifiant de l'organisation tel que défini sur data.gouv.fr.

    Returns
    -------
    list of dict
        Liste des jeux de données associés à l'organisation. Chaque élément est
        un dictionnaire représentant un dataset au format JSON.
    """
    return get(
        f"https://www.data.gouv.fr/api/1/organizations/{org_id}/datasets/",
        follow_redirects=True,
    ).json()["data"]


def list_resources_by_dataset(dataset_id: str) -> list[dict]:
    """
    Liste toutes les ressources (fichiers) associées à un jeu de données (dataset).

    Chaque dataset sur data.gouv.fr peut contenir plusieurs ressources, par exemple
    des fichiers CSV, Excel ou des liens vers des APIs.

    Parameters
    ----------
    dataset_id : str
        Identifiant du jeu de données tel que défini sur data.gouv.fr.

    Returns
    -------
    list of dict
        Liste des ressources associées au dataset. Chaque élément est un dictionnaire
        représentant une ressource au format JSON.
    """
    return get(
        f"http://www.data.gouv.fr/api/2/datasets/{dataset_id}/resources/",
        follow_redirects=True,
    ).json()["data"]


def list_resources_to_process(dataset_ids: list[str]) -> list[dict]:
    """
    Prépare la liste des ressources JSON à traiter pour un ou plusieurs jeux de données.

    Cette fonction filtre les ressources associées à chaque dataset pour ne garder que
    celles au format JSON, en excluant les fichiers dont le titre contient ".ocds".

    Chaque ressource sélectionnée est formatée avec un nom de fichier basé sur le
    dataset et le titre de la ressource, ainsi que son URL (`latest`) et un indicateur
    de traitement (`process = True`).

    Parameters
    ----------
    dataset_ids : list of str
        Liste des identifiants de jeux de données (slug ou UUID). La fonction vérifie
        que l'entrée est bien une liste de chaînes de caractères.

    Returns
    -------
    list of dict
        Liste de dictionnaires représentant les ressources à traiter. Chaque dictionnaire contient :
        - 'dataset_id' : identifiant du dataset,
        - 'resource_id' : identifiant de la ressource,
        - 'file_name' : nom de fichier généré à partir du dataset et du titre de la ressource,
        - 'url' : URL de la dernière version de la ressource (`latest`)

    Raises
    ------
    ValueError
        Si `dataset_ids` n'est pas une liste de chaînes de caractères.
    RuntimeError
        Si une erreur survient lors de l'appel à l'API de data.gouv.fr.
    """

    if not isinstance(dataset_ids, list) or not all(
        isinstance(d, str) for d in dataset_ids
    ):
        raise ValueError("dataset_ids must be a list of strings")

    resource_ids = []

    for dataset_id in dataset_ids:
        try:
            resources = list_resources_by_dataset(dataset_id)
        except Exception as e:
            raise RuntimeError(
                f"Erreur lors de la récupération des ressources du dataset '{dataset_id}': {e}"
            )

        resource_ids.extend(
            [
                {
                    "dataset_id": dataset_id,
                    "resource_id": r["id"],
                    "file_name": f"{dataset_id}-{r['title'].lower().replace('.json', '')}",
                    "url": r["latest"],
                }
                for r in resources
                if r["format"] == "json" and ".ocds" not in r["title"].lower()
            ]
        )

    return resource_ids

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


def list_resources_to_process(datasets: list[dict]) -> list[dict]:
    """
    Prépare la liste des ressources JSON à traiter pour un ou plusieurs jeux de données.

    Cette fonction filtre les ressources associées à chaque dataset pour ne garder que
    celles au format JSON, en excluant les fichiers dont le titre contient ".ocds".

    Chaque ressource sélectionnée est formatée avec un nom de fichier basé sur le
    dataset et le titre de la ressource, ainsi que son URL (`latest`) et un indicateur
    de traitement (`process = True`).

    Parameters
    ----------
    dataset_ids : list of dict
        Liste de dictionnaires contenant les identifiants des datasets à traiter.
        Chaque dictionnaire doit contenir les clés suivantes :
        - 'dataset_id' : identifiant du dataset,
        - 'incremental' : booléen indiquant si le dataset est incrémental.

    Returns
    -------
    list of dict
        Liste de dictionnaires représentant les ressources à traiter. Chaque dictionnaire contient :
        - 'dataset_id' : identifiant du dataset,
        - 'resource_id' : identifiant de la ressource,
        - 'file_name' : nom de fichier généré à partir du titre de la source et de son id,
        - 'url' : URL de la dernière version de la ressource (`latest`)

    Raises
    ------
    ValueError
        Si `dataset_ids` n'est pas une liste de chaînes de caractères.
    RuntimeError
        Si une erreur survient lors de l'appel à l'API de data.gouv.fr.
    """

    if not isinstance(datasets, list) or not all(isinstance(d, dict) for d in datasets):
        raise ValueError("dataset_ids must be a list of dictionaries")

    if not all("dataset_id" in d.keys() for d in datasets):
        raise ValueError("Each dataset must contain a 'dataset_id' key")

    if not all("incremental" in d.keys() for d in datasets):
        raise ValueError("Each dataset must contain an 'incremental' key")

    resource_ids = []

    for dataset in datasets:
        try:
            resources = list_resources_by_dataset(dataset["dataset_id"])
        except Exception as e:
            raise RuntimeError(
                f"Erreur lors de la récupération des ressources du dataset '{dataset['dataset_id']}': {e}"
            )

        for resource in resources:
            # On ne garde que les ressources au format JSON ou XML et celles qui ne sont pas des fichiers OCDS
            if (
                resource["format"] in ["json", "xml"]
                and ".ocds" not in resource["title"].lower()
            ):
                if dataset.get("incremental", False):
                    # Pour les datasets incrémentaux, on saute la ressource si elle a déjà été traitée
                    if bookmarking.is_processed(resource["id"]):
                        continue

                    resource_ids.append(
                        {
                            "dataset_id": dataset["dataset_id"],
                            "resource_id": resource["id"],
                            "file_name": f"{resource['title'].lower().replace('.json', '').replace('.xml', '').replace('.', '_')}-{resource['id']}",
                            "url": resource["latest"],
                            "file_format": resource["format"],
                        }
                    )

    return resource_ids

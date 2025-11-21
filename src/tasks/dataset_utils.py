import datetime

from httpx import get
from prefect import task
from prefect.cache_policies import INPUTS

from src.config import DATAGOUVFR_API_KEY, EXCLUDED_RESOURCES


def handle_paginated_calls(url: str) -> list[dict]:
    """
    Gère les appels paginés à l'API de data.gouv.fr pour récupérer toutes les ressources d'un dataset.

    Parameters
    ----------
    url : str
        URL de la première page des ressources du dataset.

    Returns
    -------
    list of dict
        Liste de dictionnaires représentant toutes les ressources récupérées.
    """
    data = []
    while url:
        response = get(
            url, follow_redirects=True, headers={"X-API-KEY": DATAGOUVFR_API_KEY}
        ).json()
        data.extend(response["data"])
        url = response.get("next_page")
    return data


def list_resources_by_dataset(dataset_id: str) -> list[dict]:
    """
    Liste toutes les ressources (fichiers) associées à un jeu de données (dataset).
    """

    resources = handle_paginated_calls(
        f"http://www.data.gouv.fr/api/2/datasets/{dataset_id}/resources/?page=1&page_size=50"
    )

    return resources


@task(
    retries=3,
    retry_delay_seconds=3,
    persist_result=True,
    cache_policy=INPUTS,
    cache_expiration=datetime.timedelta(hours=23),
)
def list_resources(
    datasets: list[dict], excluded_resources=EXCLUDED_RESOURCES
) -> list[dict]:
    """
    Prépare la liste des ressources JSON à traiter pour un ou plusieurs jeux de données.

    Cette fonction filtre les ressources associées à chaque dataset pour ne garder que
    celles au format JSON, en excluant les fichiers dont le titre contient ".ocds".
    """

    if not isinstance(datasets, list) or not all(isinstance(d, dict) for d in datasets):
        raise ValueError("dataset_ids must be a list of dictionaries")

    if not all("id" in d.keys() for d in datasets):
        raise ValueError("Each dataset must contain a 'id' key")

    resources = []
    all_resources = []

    for dataset in datasets:
        # Données de test .tests/data/datasets_reference_test.json
        if dataset["id"].startswith("test_"):
            all_resources += dataset["resources"]

        # Données de production ./data/datasets_reference.json
        else:
            try:
                all_resources = list_resources_by_dataset(dataset["id"])
            except Exception as e:
                raise RuntimeError(
                    f"Erreur lors de la récupération des ressources du dataset '{dataset['id']}': {e}"
                )
        for resource in all_resources:
            # On ne garde que les ressources au format JSON ou XML et celles qui ne sont pas
            # - des fichiers OCDS
            # - des fichiers XML abandonnés
            if (
                resource["format"] in ["json", "xml"]
                and resource["id"] not in excluded_resources
            ):
                resource = {
                    "dataset_id": dataset["id"],
                    "dataset_name": dataset["name"],
                    "dataset_code": dataset["code"],
                    "id": resource["id"],
                    "ori_filename": resource["title"],
                    "checksum": resource["checksum"]["value"]
                    if resource["checksum"]
                    else resource["extras"].get("analysis:checksum") or resource["id"],
                    # Dataset id en premier pour grouper les ressources d'un même dataset ensemble
                    # Nom du fichier pour le distinguer des autres fichiers du dataset
                    # Un bout d'id de ressource pour les cas où plusieurs fichiers ont le même nom dans le même dataset (ex : Occitanie)
                    "filename": f"{dataset['id']}_{resource['title'].lower().replace('.json', '').replace('.xml', '').replace('.', '_')}_{resource['id'][:3]}",
                    "url": resource["latest"],
                    "format": resource["format"],
                    "created_at": resource["created_at"],
                    "last_modified": resource["last_modified"],
                    "filesize": resource.get("filesize", None)
                    or resource["extras"].get("analysis:content-length", None)
                    or 1000,
                    "views": resource["metrics"].get("views", None),
                }
                resources.append(resource)
        print(f"- {dataset['name']}")

    return resources

from httpx import get

from config import EXCLUDED_RESOURCES


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
        response = get(url, follow_redirects=True).json()
        data.extend(response["data"])
        url = response.get("next_page")
    return data


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

    resources = handle_paginated_calls(
        f"http://www.data.gouv.fr/api/2/datasets/{dataset_id}/resources/?page=1&page_size=50"
    )

    return resources


def list_resources(datasets: list[dict]) -> list[dict]:
    """
    Prépare la liste des ressources JSON à traiter pour un ou plusieurs jeux de données.

    Cette fonction filtre les ressources associées à chaque dataset pour ne garder que
    celles au format JSON, en excluant les fichiers dont le titre contient ".ocds".

    Chaque ressource sélectionnée est formatée avec un nom de fichier basé sur le
    dataset et le titre de la ressource, ainsi que son URL (`latest`) et un indicateur
    de traitement (`process = True`).

    Parameters
    ----------
    datasets : list of dict
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
        Si `datasets` n'est pas une liste de chaînes de caractères.
    RuntimeError
        Si une erreur survient lors de l'appel à l'API de data.gouv.fr.
    """

    if not isinstance(datasets, list) or not all(isinstance(d, dict) for d in datasets):
        raise ValueError("dataset_ids must be a list of dictionaries")

    if not all("dataset_id" in d.keys() for d in datasets):
        raise ValueError("Each dataset must contain a 'dataset_id' key")

    if not all("incremental" in d.keys() for d in datasets):
        raise ValueError("Each dataset must contain an 'incremental' key")

    resources = []
    all_resources = []

    for dataset in datasets:
        # Données de test ./data/datasets_reference_test.json
        if dataset["dataset_id"].startswith("test_"):
            all_resources += dataset["resources"]

        # Données de production ./data/datasets_reference.json
        else:
            try:
                all_resources = list_resources_by_dataset(dataset["dataset_id"])
            except Exception as e:
                raise RuntimeError(
                    f"Erreur lors de la récupération des ressources du dataset '{dataset['dataset_id']}': {e}"
                )

        for resource in all_resources:
            # On ne garde que les ressources au format JSON ou XML et celles qui ne sont pas
            # - des fichiers OCDS
            # - des fichiers XML abandonnés
            if (
                resource["format"] in ["json", "xml"]
                and resource["id"] not in EXCLUDED_RESOURCES
            ):
                resources.append(
                    {
                        "dataset_id": dataset["dataset_id"],
                        "dataset_name": dataset["dataset_name"],
                        "id": resource["id"],
                        "ori_filename": resource["title"],
                        "checksum": resource["checksum"]["value"],
                        # Dataset id en premier pour grouper les ressources d'un même dataset ensemble
                        # Nom du fichier pour le distinguer des autres fichiers du dataset
                        # Un bout d'id de ressource pour les cas où plusieurs fichiers ont le même nom dans le même dataset (ex : Occitanie)
                        "file_name": f"{dataset['dataset_id']}_{resource['title'].lower().replace('.json', '').replace('.xml', '').replace('.', '_')}_{resource['id'][:3]}",
                        "url": resource["latest"],
                        "format": resource["format"],
                        "created_at": resource["created_at"],
                        "last_modified": resource["last_modified"],
                        "filesize": resource["filesize"],
                        "views": resource["metrics"].get("views", None),
                    }
                )
        print(f"- {dataset['dataset_name']}")

    return resources

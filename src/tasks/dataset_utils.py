from httpx import get

import tasks.bookmarking as bookmarking


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
    return handle_paginated_calls(
        f"http://www.data.gouv.fr/api/2/datasets/{dataset_id}/resources/?page=1&page_size=50"
    )


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
                if dataset.get("incremental", False) and bookmarking.is_processed(
                    resource["id"]
                ):
                    # Pour les datasets incrémentaux, on saute la ressource si elle a déjà été traitée
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

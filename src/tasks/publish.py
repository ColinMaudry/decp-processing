from httpx import post
import json
from os import getenv
from config import DIST_DIR

from jedi.api import project


def update_resource(api, dataset_id, resource_id, file_path, api_key):
    url = f"{api}/datasets/{dataset_id}/resources/{resource_id}/upload/"
    headers = {"X-API-KEY": api_key}
    file = {"file": open(file_path, "rb")}
    # TODO: replace requests.post with httpx.post
    response = post(url, files=file, headers=headers, timeout=120)
    return response.json()


def publish_to_datagouv(context: str):
    files = {
        "datalab": [
            "datalab.sqlite",
        ],
        "decp.info": [
            "decp-sans-titulaires.parquet",
            "decp-sans-titulaires.csv",
            "datapackage.json",
            "decp.sqlite",
        ],
    }
    api_key = getenv("DATAGOUVFR_API_KEY")
    api = "https://www.data.gouv.fr/api/1"
    dataset_id = "608c055b35eb4e6ee20eb325"

    uploads = [
        # {"file": f"{DIST_DIR}/decp.csv", "resource_id": "8587fe77-fb31-4155-8753-f6a3c5e0f5c9"},
        {
            "file": f"{DIST_DIR}/decp.parquet",
            "resource_id": "11cea8e8-df3e-4ed1-932b-781e2635e432",
            "context": context,
        },
        # {
        #     "file": f"{DIST_DIR}/decp-titulaires.csv",
        #     "resource_id": "25fcd9e6-ce5a-41a7-b6c0-f140abb2a060",
        # },
        # {
        #     "file": f"{DIST_DIR}/decp-titulaires.parquet",
        #     "resource_id": "ed8cbf31-2b86-4afc-9696-3c0d7eae5c64",
        # },
        {
            "file": f"{DIST_DIR}/decp-sans-titulaires.csv",
            "resource_id": "834c14dd-037c-4825-958d-0a841c4777ae",
            "context": "decp",
        },
        {
            "file": f"{DIST_DIR}/decp-sans-titulaires.parquet",
            "resource_id": "df28fa7d-2d36-439b-943a-351bde02f01d",
            "context": "decp",
        },
        {
            "file": f"{DIST_DIR}/datalab.sqlite",
            "resource_id": "43f54982-da60-4eb7-aaaf-ba935396209b",
            "context": "datalab",
        },
        # {
        #     "file": f"{DIST_DIR}/decp.sqlite",
        #     "resource_id": "c6b08d03-7aa4-4132-b5b2-fd76633feecc",
        #     "context": "decp",
        # },
        {
            "file": f"{DIST_DIR}/datapackage.json",
            "resource_id": "65194f6f-e273-4067-8075-56f072d56baf",
            "context": "decp",
        },
        # {"file": f"{DIST_DIR}/statistiques.csv", "resource_id": "8ded94de-3b80-4840-a5bb-7faad1c9c234"},
    ]

    for upload in uploads:
        print(f"Mise à jour de {upload['file']}...")
        if context == upload["context"]:
            print(
                json.dumps(
                    update_resource(
                        api, dataset_id, upload["resource_id"], upload["file"], api_key
                    ),
                    indent=4,
                )
            )

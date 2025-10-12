from httpx import post

from config import DATAGOUVFR_API, DATAGOUVFR_API_KEY, DIST_DIR


def update_resource(dataset_id, resource_id, file_path, api_key):
    url = f"{DATAGOUVFR_API}/datasets/{dataset_id}/resources/{resource_id}/upload/"
    headers = {"X-API-KEY": api_key}
    file = {"file": open(file_path, "rb")}
    response = post(url, files=file, headers=headers, timeout=120).raise_for_status()
    return response.json()


def publish_to_datagouv(context: str):
    dataset_id = "608c055b35eb4e6ee20eb325"

    uploads = [
        # {"file": str(DIST_DIR/"decp.csv"), "resource_id": "8587fe77-fb31-4155-8753-f6a3c5e0f5c9"},
        {
            "file": str(DIST_DIR / "decp.parquet"),
            "resource_id": "11cea8e8-df3e-4ed1-932b-781e2635e432",
        },
        {
            "file": str(DIST_DIR / "decp.csv"),
            "resource_id": "22847056-61df-452d-837d-8b8ceadbfc52",
        },
        # {
        #     "file": str(DIST_DIR/"decp-titulaires.csv"),
        #     "resource_id": "25fcd9e6-ce5a-41a7-b6c0-f140abb2a060",
        # },
        # {
        #     "file": str(DIST_DIR/"decp-titulaires.parquet"),
        #     "resource_id": "ed8cbf31-2b86-4afc-9696-3c0d7eae5c64",
        # },
        {
            "file": str(DIST_DIR / "decp-sans-titulaires.csv"),
            "resource_id": "834c14dd-037c-4825-958d-0a841c4777ae",
        },
        {
            "file": str(DIST_DIR / "decp-sans-titulaires.parquet"),
            "resource_id": "df28fa7d-2d36-439b-943a-351bde02f01d",
        },
        {
            "file": str(DIST_DIR / "decp.sqlite"),
            "resource_id": "43f54982-da60-4eb7-aaaf-ba935396209b",
        },
        {
            "file": str(DIST_DIR / "schema.json"),
            "resource_id": "9a4144c0-ee44-4dec-bee5-bbef38191d9a",
        },
        {
            "file": str(DIST_DIR / "statistiques.csv"),
            "resource_id": "8ded94de-3b80-4840-a5bb-7faad1c9c234",
        },
    ]

    for upload in uploads:
        print(f"Mise à jour de {upload['file']}...")
        result = update_resource(
            dataset_id, upload["resource_id"], upload["file"], DATAGOUVFR_API_KEY
        )
        if result["success"] is True:
            print("OK")


def publish_scrap_to_datagouv(year: str, file_path):
    dataset_id = "68ebb48dd708fdb2d7c15bff"
    resources = {"2025": "8001b9e5-94bd-484e-b351-f9cbe82f8d84"}
    print(f"Mise à jour des données marches-securises.fr de {year}...")
    result = update_resource(dataset_id, resources[year], file_path, DATAGOUVFR_API_KEY)
    if result["success"] is True:
        print("OK")

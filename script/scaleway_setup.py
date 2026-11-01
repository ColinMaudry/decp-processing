#!/usr/bin/env python3
"""Installation idempotente des ressources permanentes Scaleway et Prefect (#189).

À exécuter sur le serveur actuel : `uv run python script/scaleway_setup.py`.
Relancer le script ne crée rien : chaque ressource existante est réutilisée et
l'état final est affiché (ids des volumes, work pools, limites de concurrence).

Prérequis, dans le `.env` du serveur : SCW_ACCESS_KEY, SCW_SECRET_KEY,
SCW_DEFAULT_PROJECT_ID, SCW_DEFAULT_ZONE, PREFECT_API_URL.

Ressources créées :
- volumes Block Storage `decp-data-prod` et `decp-data-dev` (30 Go, perf_iops 5000),
  dans la même zone que les futures instances ;
- work pools Prefect `scaleway-prod` et `scaleway-dev` (type process) ;
- limites de concurrence globales Prefect `volume-prod` et `volume-dev` (valeur 1),
  utilisées par le lanceur (`prefect.concurrency.sync.concurrency`).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import find_dotenv, load_dotenv
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import (
    GlobalConcurrencyLimitCreate,
    WorkPoolCreate,
)
from prefect.exceptions import ObjectNotFound
from scaleway import ALL_ZONES, Client
from scaleway.block.v1alpha1 import BlockV1Alpha1API
from scaleway.block.v1alpha1.types import CreateVolumeRequestFromEmpty
from scaleway.instance.v1.custom_api import InstanceUtilsV1API
from scaleway.instance.v1.types import ServerTypesAvailability
from scaleway_core.api import ScalewayException

from src.config import SCALEWAY_DATA_VOLUME_NAMES

# Le .env du serveur (SCW_*, PREFECT_API_URL) doit être chargé avant la
# construction des clients Scaleway et Prefect.
load_dotenv(find_dotenv(), override=False)

TYPE_INSTANCE = "MEMORY3-X8C-64G"
TAILLE_VOLUME = 30_000_000_000  # octets (30 Go)
PERF_IOPS = 5000
NOMS_VOLUMES = tuple(SCALEWAY_DATA_VOLUME_NAMES.values())
NOMS_POOLS = ("scaleway-prod", "scaleway-dev")
NOMS_LIMITES = ("volume-prod", "volume-dev")
LIMITE_VOLUME = 1


def _zones_ou_le_type_est_disponible(api: InstanceUtilsV1API) -> list[str]:
    """Zones dans lesquelles TYPE_INSTANCE est réellement disponible."""
    zones = []
    for zone in ALL_ZONES:
        try:
            reponse = api.get_server_types_availability(zone=zone)
        except ScalewayException as e:
            if e.status_code in (401, 403):
                # Erreur d'authentification ou de permission : ce n'est pas
                # la zone qui est indisponible, le script doit s'arrêter.
                raise
            continue  # zone sans disponibilité pour ce type d'instance
        disponibilite = reponse.servers.get(TYPE_INSTANCE)
        if (
            disponibilite is not None
            and disponibilite.availability == ServerTypesAvailability.AVAILABLE
        ):
            zones.append(zone)
    return zones


def verifier_disponibilite(client: Client, zone: str) -> None:
    """Vérifie que TYPE_INSTANCE est disponible dans `zone`, sinon s'arrête."""
    api = InstanceUtilsV1API(client)
    reponse = api.get_server_types_availability(zone=zone)
    disponibilite = reponse.servers.get(TYPE_INSTANCE)
    if (
        disponibilite is not None
        and disponibilite.availability == ServerTypesAvailability.AVAILABLE
    ):
        return
    zones = _zones_ou_le_type_est_disponible(api)
    raise SystemExit(
        f"{TYPE_INSTANCE} n'est pas disponible dans la zone {zone}.\n"
        f"Zones où essayer : {', '.join(zones) if zones else 'aucune'}.\n"
        "Mettez à jour SCW_DEFAULT_ZONE dans le .env du serveur, puis relancez "
        "le script (les volumes doivent être dans la même zone que les instances)."
    )


def assurer_volume(api: BlockV1Alpha1API, zone: str, nom: str) -> str:
    """Retourne l'id du volume `nom`, créé s'il n'existe pas."""
    volumes = api.list_volumes(zone=zone, name=nom).volumes
    if volumes:
        return volumes[0].id
    volume = api.create_volume(
        zone=zone,
        name=nom,
        perf_iops=PERF_IOPS,
        from_empty=CreateVolumeRequestFromEmpty(size=TAILLE_VOLUME),
    )
    print(f"Volume créé : {nom}")
    return volume.id


def assurer_pool(client, nom: str):
    """Retourne le work pool `nom` (type process), créé s'il n'existe pas."""
    try:
        return client.read_work_pool(nom)
    except ObjectNotFound:
        pool = client.create_work_pool(WorkPoolCreate(name=nom, type="process"))
        print(f"Work pool créé : {nom}")
        return pool


def assurer_limite(client, nom: str):
    """Retourne la limite de concurrence globale `nom` (valeur 1), créée sinon."""
    try:
        return client.read_global_concurrency_limit_by_name(nom)
    except ObjectNotFound:
        client.create_global_concurrency_limit(
            GlobalConcurrencyLimitCreate(name=nom, limit=LIMITE_VOLUME)
        )
        print(f"Limite de concurrence créée : {nom} ({LIMITE_VOLUME})")
        return client.read_global_concurrency_limit_by_name(nom)


def main() -> None:
    client_scw = Client.from_config_file_and_env()
    zone = client_scw.default_zone
    if not zone:
        raise SystemExit("SCW_DEFAULT_ZONE est absente du .env du serveur.")
    print(f"Zone Scaleway : {zone}")
    verifier_disponibilite(client_scw, zone)

    block_api = BlockV1Alpha1API(client_scw)
    ids_volumes = {nom: assurer_volume(block_api, zone, nom) for nom in NOMS_VOLUMES}

    with get_client() as client_prefect:
        pools = {nom: assurer_pool(client_prefect, nom) for nom in NOMS_POOLS}
        limites = {nom: assurer_limite(client_prefect, nom) for nom in NOMS_LIMITES}

    print("\nÉtat final :")
    for nom, id_volume in ids_volumes.items():
        print(f"  volume {nom} : {id_volume}")
    for nom, pool in pools.items():
        print(f"  work pool {nom} : {pool.type} ({pool.id})")
    for nom, limite in limites.items():
        print(f"  limite {nom} : {limite.limit} ({limite.id})")


if __name__ == "__main__":
    main()

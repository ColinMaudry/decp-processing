"""Couche d'accès à Scaleway : création et destruction de l'instance éphémère.

Aucune logique Prefect ici. Le user-data n'est jamais journalisé.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from scaleway import Client
from scaleway.block.v1alpha1 import BlockV1Alpha1API
from scaleway.instance.v1 import Server, ServerAction, VolumeServerTemplate
from scaleway.instance.v1.custom_api import InstanceUtilsV1API
from scaleway_core.api import ScalewayException

TAILLE_VOLUME_BOOT = 40_000_000_000  # octets


@dataclass(frozen=True)
class Instance:
    id: str
    ip: str | None
    tags: list[str]
    created_at: datetime  # UTC, timezone-aware


def _utc(d: datetime) -> datetime:
    return (
        d.replace(tzinfo=timezone.utc)
        if d.tzinfo is None
        else d.astimezone(timezone.utc)
    )


def _instance(server: Server) -> Instance:
    ip = None
    if server.public_ip is not None:
        ip = server.public_ip.address
    elif server.public_ips:
        ip = server.public_ips[0].address
    return Instance(
        id=server.id,
        ip=ip,
        tags=list(server.tags or []),
        created_at=_utc(server.creation_date),
    )


class ScalewayProvider:
    def __init__(self, instance_api=None, block_api=None, zone=None):
        if instance_api is None or block_api is None:
            client = Client.from_config_file_and_env()
            instance_api = instance_api or InstanceUtilsV1API(client)
            block_api = block_api or BlockV1Alpha1API(client)
            zone = zone or client.default_zone
        self.instance_api = instance_api
        self.block_api = block_api
        self.zone = zone

    def __repr__(self) -> str:
        return f"ScalewayProvider(zone={self.zone!r})"

    def _attendre(self, server_id: str) -> Server:
        """Attend un état stable (running/stopped) ; surchargeable pour les tests."""
        return self.instance_api.wait_instance_server(
            server_id=server_id, zone=self.zone
        ).server

    def create_instance(
        self,
        *,
        name: str,
        instance_type: str,
        data_volume_id: str,
        user_data: str,
        tags: list[str],
    ) -> Instance:
        api = self.instance_api
        server = api.create_server(
            zone=self.zone,
            commercial_type=instance_type,
            name=name,
            dynamic_ip_required=True,
            image="debian_bookworm",
            volumes={
                "0": VolumeServerTemplate(
                    volume_type="sbs_volume",
                    size=TAILLE_VOLUME_BOOT,
                    boot=True,
                    name=f"{name}-boot",
                ),
                "1": VolumeServerTemplate(id=data_volume_id, volume_type="sbs_volume"),
            },
            tags=tags,
        ).server
        api.set_server_user_data(
            server_id=server.id,
            key="cloud-init",
            content=user_data.encode("utf-8"),
            zone=self.zone,
        )
        api.server_action(
            server_id=server.id, zone=self.zone, action=ServerAction.POWERON
        )
        server = self._attendre(server.id)
        if server.state != "running":
            raise RuntimeError(
                f"L'instance {server.id} n'a pas démarré (état : {server.state})"
            )
        # L'IP dynamique n'est connue qu'après démarrage.
        server = api.get_server(server_id=server.id, zone=self.zone).server
        return _instance(server)

    def delete_instance(self, instance_id: str, data_volume_id: str) -> None:
        api = self.instance_api
        boot = api.get_server(server_id=instance_id, zone=self.zone).server.volumes.get(
            "0"
        )
        boot_id = boot.id if boot is not None else None
        api.server_action(
            server_id=instance_id, zone=self.zone, action=ServerAction.POWEROFF
        )
        self._attendre(instance_id)
        api.detach_server_volume(
            server_id=instance_id, volume_id=data_volume_id, zone=self.zone
        )
        api.server_action(
            server_id=instance_id, zone=self.zone, action=ServerAction.TERMINATE
        )
        if boot_id is not None:
            try:
                self.block_api.delete_volume(volume_id=boot_id, zone=self.zone)
            except ScalewayException as e:
                if e.status_code != 404:
                    raise

    def list_instances(self, tags: list[str]) -> list[Instance]:
        servers = self.instance_api.list_servers(zone=self.zone, tags=tags).servers
        return [_instance(s) for s in servers]

    def volume_id(self, name: str) -> str:
        volumes = self.block_api.list_volumes(zone=self.zone, name=name).volumes
        for v in volumes:
            if v.name == name:
                return v.id
        raise LookupError(f"Volume introuvable : {name}")

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from scaleway.instance.v1 import ServerAction

from src.infra.scaleway import Instance, ScalewayProvider


def _api_pour_delete(state="running"):
    """API factice dont get_server reflète `state` et l'attente renvoie stopped."""
    api = MagicMock()
    serveur = _serveur(state)
    serveur.volumes = {"0": MagicMock(id="vol-boot")}
    api.get_server.return_value.server = serveur
    api.wait_instance_server.return_value.server = _serveur("stopped")
    return api


def test_delete_instance_detache_avant_terminate():
    api = _api_pour_delete()
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    p.delete_instance("srv", "vol-data")
    calls = [c[0] for c in api.method_calls]
    assert (
        calls.index("detach_server_volume")
        < [
            i
            for i, c in enumerate(api.method_calls)
            if c[0] == "server_action" and c.kwargs["action"] == ServerAction.TERMINATE
        ][0]
    )
    api.detach_server_volume.assert_called_once_with(
        server_id="srv", volume_id="vol-data", zone="fr-par-1"
    )


def test_delete_instance_supprime_volume_boot():
    api = _api_pour_delete()
    block = MagicMock()
    p = ScalewayProvider(instance_api=api, block_api=block, zone="fr-par-1")
    p.delete_instance("srv", "vol-data")
    block.delete_volume.assert_called_once_with(volume_id="vol-boot", zone="fr-par-1")


def test_delete_instance_deja_stopped_detruit_sans_poweroff():
    api = _api_pour_delete(state="stopped")
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    p.delete_instance("srv", "vol-data")
    actions = [c.kwargs["action"] for c in api.method_calls if c[0] == "server_action"]
    assert ServerAction.POWEROFF not in actions
    assert ServerAction.TERMINATE in actions
    api.detach_server_volume.assert_called_once_with(
        server_id="srv", volume_id="vol-data", zone="fr-par-1"
    )


def test_delete_instance_running_poweroff_puis_stopped_avant_detachement():
    api = _api_pour_delete(state="running")
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    p.delete_instance("srv", "vol-data")
    noms = [c[0] for c in api.method_calls]
    i_poweroff = [
        i
        for i, c in enumerate(api.method_calls)
        if c[0] == "server_action" and c.kwargs["action"] == ServerAction.POWEROFF
    ][0]
    assert i_poweroff < noms.index("wait_instance_server")
    assert noms.index("wait_instance_server") < noms.index("detach_server_volume")


def test_delete_instance_rejete_un_serveur_revenu_running():
    api = _api_pour_delete(state="running")
    api.wait_instance_server.return_value.server = _serveur("running")
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    with pytest.raises(RuntimeError, match="stopped"):
        p.delete_instance("srv", "vol-data")
    api.detach_server_volume.assert_not_called()


def _serveur(state="running"):
    s = MagicMock()
    s.id = "srv"
    s.state = state
    s.tags = ["decp"]
    s.creation_date = datetime(2026, 10, 31, 12, 0)
    s.public_ip.address = "1.2.3.4"
    return s


def test_create_instance_ordre_et_parametres():
    api = MagicMock()
    api.create_server.return_value.server = _serveur("stopped")
    api.wait_instance_server.return_value.server = _serveur("running")
    api.get_server.return_value.server = _serveur("running")
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")

    inst = p.create_instance(
        name="decp",
        instance_type="POP2-HM-8C-64G",
        data_volume_id="vol-data",
        user_data="#cloud-config secret",
        tags=["decp"],
    )

    noms = [c[0] for c in api.method_calls]
    i_create = noms.index("create_server")
    i_ud = noms.index("set_server_user_data")
    i_on = [
        i
        for i, c in enumerate(api.method_calls)
        if c[0] == "server_action" and c.kwargs["action"] == ServerAction.POWERON
    ][0]
    assert i_create < i_ud < i_on
    api.set_server_user_data.assert_called_once_with(
        server_id="srv",
        key="cloud-init",
        content=b"#cloud-config secret",
        zone="fr-par-1",
    )
    kw = api.create_server.call_args.kwargs
    assert kw["dynamic_ip_required"] is True
    assert kw["image"] == "debian_bookworm"
    assert kw["commercial_type"] == "POP2-HM-8C-64G"
    assert kw["volumes"]["0"].boot is True
    assert kw["volumes"]["0"].name == "decp-boot"
    assert kw["volumes"]["0"].size == 40_000_000_000
    assert kw["volumes"]["1"].id == "vol-data"
    assert inst == Instance(
        id="srv",
        ip="1.2.3.4",
        tags=["decp"],
        created_at=datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc),
    )
    assert "secret" not in repr(p)


def test_create_instance_echec_si_pas_running():
    api = MagicMock()
    api.create_server.return_value.server = _serveur("stopped")
    api.wait_instance_server.return_value.server = _serveur("stopped")
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    with pytest.raises(RuntimeError):
        p.create_instance(
            name="decp",
            instance_type="t",
            data_volume_id="v",
            user_data="x",
            tags=[],
        )


def test_list_instances():
    api = MagicMock()
    api.list_servers.return_value.servers = [_serveur()]
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    assert [i.id for i in p.list_instances(["decp"])] == ["srv"]
    api.list_servers.assert_called_once_with(zone="fr-par-1", tags=["decp"])


def test_volume_id():
    block = MagicMock()
    vol = MagicMock(id="vol-1")
    vol.name = "decp-data"
    block.list_volumes.return_value.volumes = [vol]
    p = ScalewayProvider(instance_api=MagicMock(), block_api=block, zone="fr-par-1")
    assert p.volume_id("decp-data") == "vol-1"
    block.list_volumes.return_value.volumes = []
    with pytest.raises(LookupError):
        p.volume_id("absent")

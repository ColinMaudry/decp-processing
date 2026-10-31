"""Tests du lanceur de run distant : fonction pure, sans réseau ni Prefect.

L'horloge est simulée : `now()` avance d'un `poll_interval` à chaque `sleep()`.
"""

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from src.infra.remote_run import (
    RemoteRunConfig,
    RemoteRunError,
    RunState,
    cleanup_orphans,
    execute_remote_run,
)
from src.infra.scaleway import Instance

POLL = timedelta(seconds=30)
MAINTENANT = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def _instance(id: str, env: str, cree_le: datetime) -> Instance:
    return Instance(
        id=id,
        ip="1.2.3.4",
        tags=["decp-processing", f"env:{env}"],
        created_at=cree_le,
    )


class FakeProvider:
    """Fournisseur qui enregistre chaque appel ; peut refuser la création."""

    def __init__(self, instances=None):
        self.instances = list(instances or [])
        self.calls = []
        self.refus_creation = False
        self.sans_ip = False

    def create_instance(self, *, name, instance_type, data_volume_id, user_data, tags):
        self.calls.append(
            ("create_instance", name, instance_type, data_volume_id, tags)
        )
        if self.refus_creation:
            raise RuntimeError("quota dépassé")
        instance = Instance(
            id=f"srv-{len(self.instances)}",
            ip=None if self.sans_ip else "1.2.3.4",
            tags=list(tags),
            created_at=MAINTENANT,
        )
        self.instances.append(instance)
        return instance

    def delete_instance(self, instance_id, data_volume_id):
        self.calls.append(("delete_instance", instance_id, data_volume_id))
        self.instances = [i for i in self.instances if i.id != instance_id]

    def list_instances(self, tags):
        return [i for i in self.instances if all(t in i.tags for t in tags)]

    def volume_id(self, name):
        return f"vol-{name}"


class Horloge:
    """now() avance d'un poll_interval à chaque sleep()."""

    def __init__(self, poll_interval=POLL):
        self.instant = MAINTENANT
        self.poll_interval = poll_interval

    def now(self) -> datetime:
        return self.instant

    def sleep(self, _secondes: float) -> None:
        self.instant += self.poll_interval


def _config(
    dist_dir=None,
    env="prod",
    user_data="#!/bin/bash\nexit 0",
    start_timeout=timedelta(minutes=5),
    max_duration=timedelta(hours=4),
    volume_names=None,
) -> RemoteRunConfig:
    return RemoteRunConfig(
        env=env,
        deployment="dist",
        launcher_run_id="run-abc",
        instance_type="GP1-XS",
        data_volume_id="vol-data",
        volume_names=volume_names
        if volume_names is not None
        else {"prod": "decp-data-prod", "dev": "decp-data-dev"},
        user_data=user_data,
        dist_dir=dist_dir,
        start_timeout=start_timeout,
        max_duration=max_duration,
        poll_interval=POLL,
    )


def _etats(etats):
    """get_state qui déroule la séquence puis reste sur le dernier état."""
    restants = list(etats)

    def get_state(run_id):
        if len(restants) > 1:
            return restants.pop(0)
        return restants[0]

    return get_state


def _lancer(
    cfg,
    provider,
    *,
    etats,
    sync_dist=None,
    trigger=None,
    horloge=None,
    secret_strings=None,
    boot_log="sortie de cloud-init",
    fetch=None,
):
    """Câble les dépendances et exécute le run ; capture la RemoteRunError."""
    horloge = horloge if horloge is not None else Horloge()
    res = SimpleNamespace(
        instance=None, erreur=None, trigger=[], cancel=[], sync=[], boot=[]
    )

    def _trigger():
        res.trigger.append(True)
        return "run-1"

    def _cancel(run_id):
        res.cancel.append(run_id)

    def _sync(ip, cible):
        res.sync.append((ip, cible))

    def _boot(run_id):
        res.boot.append(run_id)
        return boot_log

    try:
        res.instance = execute_remote_run(
            cfg,
            provider=provider,
            trigger=trigger or _trigger,
            get_state=_etats(etats),
            cancel=_cancel,
            sync_dist=sync_dist or _sync,
            fetch_boot_log=fetch or _boot,
            now=horloge.now,
            sleep=horloge.sleep,
            logger=logging.getLogger("test.remote_run"),
            secret_strings=secret_strings,
        )
    except RemoteRunError as exc:
        res.erreur = exc
    return res


def test_succes_rapatrie_bascule_et_detruit(tmp_path):
    dist = tmp_path / "dist"
    provider = FakeProvider()

    appels_sync = []

    def sync(ip, cible):
        appels_sync.append((ip, cible))
        cible.mkdir(parents=True)
        (cible / "donnees.csv").write_text("ok")

    res = _lancer(
        _config(dist_dir=dist),
        provider,
        etats=[RunState.PENDING, RunState.RUNNING, RunState.COMPLETED],
        sync_dist=sync,
    )
    assert res.erreur is None
    assert appels_sync == [("1.2.3.4", tmp_path / "dist_wip")]
    assert (dist / "donnees.csv").read_text() == "ok"
    assert not (tmp_path / "dist_wip").exists()
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]
    assert provider.calls[1] == ("delete_instance", res.instance.id, "vol-data")


def test_echec_du_run_detruit_sans_rapatrier():
    provider = FakeProvider()
    res = _lancer(_config(), provider, etats=[RunState.FAILED])
    assert isinstance(res.erreur, RemoteRunError)
    assert res.sync == []
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]


def test_delai_de_demarrage_depasse_annule_logue_et_detruit(caplog):
    caplog.set_level(logging.INFO)
    provider = FakeProvider()
    # 45 s de délai, un poll de 30 s : échec au 3e sondage (60 s)
    res = _lancer(
        _config(start_timeout=timedelta(seconds=45)),
        provider,
        etats=[RunState.PENDING],
    )
    assert isinstance(res.erreur, RemoteRunError)
    assert res.cancel == ["run-1"]
    assert res.boot == ["run-1"]
    assert "Délai de démarrage" in caplog.text
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]


def test_duree_maximale_depassee_annule_et_detruit():
    provider = FakeProvider()
    # 45 s de durée maximale, un poll de 30 s : échec au 3e sondage (60 s)
    res = _lancer(
        _config(max_duration=timedelta(seconds=45)),
        provider,
        etats=[RunState.RUNNING],
    )
    assert isinstance(res.erreur, RemoteRunError)
    assert res.cancel == ["run-1"]
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]


def test_echec_de_creation_ne_declenche_rien():
    provider = FakeProvider()
    provider.refus_creation = True

    def trigger_interdit():
        raise AssertionError("trigger ne doit pas être appelé")

    with pytest.raises(RuntimeError, match="quota"):
        _lancer(
            _config(), provider, etats=[RunState.COMPLETED], trigger=trigger_interdit
        )
    assert [c[0] for c in provider.calls] == ["create_instance"]


def test_echec_du_rsync_ne_bascule_pas(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "ancien.csv").write_text("ancien")
    provider = FakeProvider()

    def sync_qui_echoue(ip, cible):
        raise RuntimeError("rsync: connexion refusée")

    res = _lancer(
        _config(dist_dir=dist),
        provider,
        etats=[RunState.COMPLETED],
        sync_dist=sync_qui_echoue,
    )
    assert isinstance(res.erreur, RemoteRunError)
    assert (dist / "ancien.csv").read_text() == "ancien"
    assert not (tmp_path / "dist_wip").exists()
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]


def test_sans_dist_dir_pas_de_rsync():
    res = _lancer(_config(dist_dir=None), FakeProvider(), etats=[RunState.COMPLETED])
    assert res.erreur is None
    assert res.sync == []


def test_orphelins_de_l_env_detruits_avant_creation():
    orphelin = Instance(
        id="orphee",
        ip="1.2.3.4",
        tags=["decp-processing", "env:prod", "flow-run:vieux"],
        created_at=MAINTENANT,
    )
    provider = FakeProvider([orphelin])
    res = _lancer(_config(env="prod"), provider, etats=[RunState.COMPLETED])
    assert res.erreur is None
    noms = [c[0] for c in provider.calls]
    assert noms.index("delete_instance") < noms.index("create_instance")
    assert provider.calls[0] == ("delete_instance", "orphee", "vol-decp-data-prod")
    assert provider.instances == []


def test_orphelin_d_un_autre_env_trop_ancien_detruit():
    vieux = _instance("vieux", "dev", MAINTENANT - timedelta(hours=5))
    recent = _instance("recent", "dev", MAINTENANT - timedelta(hours=1))
    vieux_staging = _instance(
        "vieux-staging", "staging", MAINTENANT - timedelta(hours=5)
    )
    vieux_inconnu = _instance(
        "vieux-inconnu", "bizarre", MAINTENANT - timedelta(hours=5)
    )
    provider = FakeProvider([vieux, recent, vieux_staging, vieux_inconnu])

    detruits = cleanup_orphans(
        provider,
        "prod",
        MAINTENANT,
        timedelta(hours=4),
        {
            "prod": "decp-data-prod",
            "dev": "decp-data-dev",
            "staging": "decp-data-staging",
        },
    )
    # Le trop ancien de dev est détruit avec son vrai volume (résolu par nom),
    # le récent conservé ; celui de staging aussi ; l'env inconnu du mapping,
    # sans volume détachable connu, est ignoré plutôt que détruit à l'aveugle.
    assert detruits == ["vieux", "vieux-staging"]
    assert [i.id for i in provider.instances] == ["recent", "vieux-inconnu"]
    assert provider.calls[0] == ("delete_instance", "vieux", "vol-decp-data-dev")
    assert (
        "delete_instance",
        "vieux-staging",
        "vol-decp-data-staging",
    ) in provider.calls
    # Aucune destruction ne reçoit un volume vide
    assert all(c[2] != "" for c in provider.calls)


def test_aucun_secret_dans_les_logs(caplog):
    caplog.set_level(logging.INFO)
    secret = "secret-123"
    user_data = f"#!/bin/bash\necho {secret}"

    # Échec du run
    res = _lancer(_config(user_data=user_data), FakeProvider(), etats=[RunState.FAILED])
    assert isinstance(res.erreur, RemoteRunError)

    # Timeout de démarrage : le boot log peut échoer le user-data, il est masqué
    res = _lancer(
        _config(user_data=user_data, start_timeout=timedelta(seconds=45)),
        FakeProvider(),
        etats=[RunState.PENDING],
        secret_strings=[secret],
        boot_log=f"cloud-init: echo {secret}",
    )
    assert isinstance(res.erreur, RemoteRunError)

    # Durée maximale
    res = _lancer(
        _config(user_data=user_data, max_duration=timedelta(seconds=45)),
        FakeProvider(),
        etats=[RunState.RUNNING],
        secret_strings=[secret],
    )
    assert isinstance(res.erreur, RemoteRunError)

    assert secret not in caplog.text
    assert secret not in str(res.erreur)
    assert "***" in caplog.text


def test_retour_en_pending_reinitialise_le_delai_de_demarrage():
    # 45 s de délai, un poll de 30 s : sans réinitialisation au retour en
    # PENDING (t=60 s, mesuré depuis trigger()), le délai serait dépassé.
    res = _lancer(
        _config(start_timeout=timedelta(seconds=45)),
        FakeProvider(),
        etats=[
            RunState.PENDING,
            RunState.RUNNING,
            RunState.PENDING,
            RunState.COMPLETED,
        ],
    )
    assert res.erreur is None


def test_instance_sans_ip_echoue_sans_rapatrier(tmp_path):
    provider = FakeProvider()
    provider.sans_ip = True
    res = _lancer(
        _config(dist_dir=tmp_path / "dist"), provider, etats=[RunState.COMPLETED]
    )
    assert isinstance(res.erreur, RemoteRunError)
    assert res.sync == []
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]


def test_echec_du_boot_log_ne_masque_pas_le_diagnostic(caplog):
    caplog.set_level(logging.INFO)
    provider = FakeProvider()

    def fetch_qui_echoue(run_id):
        raise RuntimeError("ssh en panne")

    res = _lancer(
        _config(start_timeout=timedelta(seconds=45)),
        provider,
        etats=[RunState.PENDING],
        fetch=fetch_qui_echoue,
    )
    assert isinstance(res.erreur, RemoteRunError)
    assert "Délai de démarrage" in caplog.text
    assert "ssh en panne" in caplog.text
    assert [c[0] for c in provider.calls] == ["create_instance", "delete_instance"]

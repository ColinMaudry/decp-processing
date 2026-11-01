"""Flow lanceur : exécute un deployment sur une instance Scaleway éphémère.

Le flow crée l'instance, déclenche le run distant, attend sa fin, rapatrie
le répertoire dist si demandé, puis détruit l'instance dans tous les cas
(cycle de vie délégué à `src.infra.remote_run`).
"""

import os
import shlex
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from uuid import UUID

from prefect import flow, get_client
from prefect.concurrency.sync import concurrency
from prefect.context import get_run_context
from prefect.deployments import run_deployment
from prefect.logging import get_logger
from prefect.states import Cancelling
from tenacity import retry, stop_after_attempt, wait_fixed

from src.config import (
    INSTANCE_ENV_VARS,
    SCALEWAY_DATA_VOLUME_NAMES,
    SCALEWAY_SSH_KEY_PATH,
)
from src.flows.notify import notify_exception_by_email
from src.infra.remote_run import RemoteRunConfig, RunState, execute_remote_run
from src.infra.scaleway import Instance, ScalewayProvider
from src.infra.user_data import render_user_data

logger = get_logger(__name__)

SSH_OPTIONS = [
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "ConnectTimeout=30",
]
BOOT_LOG_PATH = "/var/log/cloud-init-output.log"


class ProviderAvecRetry:
    """Adaptateur de `ScalewayProvider` : création d'instance avec retry.

    `create_instance` est réessayé 2 fois (3 tentatives) à 60 s d'intervalle ;
    les autres méthodes délèguent telles quelles.
    """

    def __init__(self, provider: ScalewayProvider):
        self._provider = provider

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
    def create_instance(self, **kwargs) -> Instance:
        return self._provider.create_instance(**kwargs)

    def delete_instance(self, instance_id: str, data_volume_id: str) -> None:
        self._provider.delete_instance(instance_id, data_volume_id)

    def list_instances(self, tags: list[str]) -> list[Instance]:
        return self._provider.list_instances(tags)

    def volume_id(self, name: str) -> str:
        return self._provider.volume_id(name)


def _ssh_command(ip: str) -> list[str]:
    return [
        "ssh",
        "-i",
        str(SCALEWAY_SSH_KEY_PATH),
        *SSH_OPTIONS,
        f"root@{ip}",
    ]


def _public_key() -> str:
    """Clé publique correspondant à SCALEWAY_SSH_KEY_PATH."""
    proc = subprocess.run(
        ["ssh-keygen", "-y", "-f", str(SCALEWAY_SSH_KEY_PATH)],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _make_sync_dist() -> Callable[[str, Path], None]:
    """Rapatrie /work/dist de l'instance vers un répertoire wip local."""

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
    def sync_dist(ip: str, wip: Path) -> None:
        remote_shell = " ".join(
            shlex.quote(part)
            for part in ["ssh", "-i", str(SCALEWAY_SSH_KEY_PATH), *SSH_OPTIONS]
        )
        cmd = [
            "rsync",
            "--delete",
            "--archive",
            "-e",
            remote_shell,
            f"root@{ip}:/work/dist/",
            str(wip),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            # Le stderr est tronqué : il peut contenir des chemins ou des
            # contenus de fichiers, jamais journalisés en entier.
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, stderr=(proc.stderr or "")[:500]
            )

    return sync_dist


def _make_fetch_boot_log(provider: ScalewayProvider, launcher_run_id: str):
    """Lit le boot log de l'instance, par ssh ; erreurs ignorées."""

    def fetch_boot_log(run_id: str) -> str:
        try:
            instances = provider.list_instances([f"flow-run:{launcher_run_id}"])
            ip = instances[0].ip
            if ip is None:
                return "boot log indisponible"
            proc = subprocess.run(
                [*_ssh_command(ip), f"tail -n 200 {BOOT_LOG_PATH}"],
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
            )
            return proc.stdout
        except Exception:
            return "boot log indisponible"

    return fetch_boot_log


def _make_trigger(deployment: str, env: str):
    def trigger() -> str:
        run = run_deployment(
            deployment,
            timeout=0,
            as_subflow=True,
            flow_run_name=f"decp-{env}-{deployment}",
        )
        return str(run.id)

    return trigger


def _get_state(run_id: str) -> RunState:
    with get_client() as client:
        state = client.read_flow_run(UUID(run_id)).state
    if state is None:
        return RunState.PENDING
    nom = state.name
    if nom in ("Scheduled", "Pending", "AwaitingRetry", "Retrying", "Cancelling"):
        return RunState.PENDING
    if nom == "Running":
        return RunState.RUNNING
    if nom == "Completed":
        return RunState.COMPLETED
    return RunState.FAILED


def _cancel(run_id: str) -> None:
    with get_client() as client:
        client.set_flow_run_state(UUID(run_id), Cancelling())


@flow(log_prints=True)
def run_on_scaleway(
    env: str,
    deployment: str,
    branch: str,
    dist_dir: str | None,
    instance_type: str = "MEMORY3-X8C-64G",
    start_timeout_minutes: int = 20,
    max_duration_hours: int = 4,
) -> None:
    """Exécute `deployment` (branche `branch`) sur une instance éphémère."""
    launcher_run_id = str(get_run_context().flow_run.id)
    with concurrency(f"volume-{env}", raise_on_lease_renewal_failure=False):
        provider = ScalewayProvider()
        data_volume_id = provider.volume_id(SCALEWAY_DATA_VOLUME_NAMES[env])

        env_vars = {
            "DECP_DATA_DIR": "/data",
            "PREFECT_HOME": "/data/.prefect",
            "DECP_DIST_DIR": "/work/dist",
        }
        for name in INSTANCE_ENV_VARS:
            if os.environ.get(name):
                env_vars[name] = os.environ[name]

        user_data = render_user_data(
            env=env,
            branch=branch,
            volume_id=data_volume_id,
            ssh_public_key=_public_key(),
            env_vars=env_vars,
        )

        cfg = RemoteRunConfig(
            env=env,
            deployment=deployment,
            launcher_run_id=launcher_run_id,
            instance_type=instance_type,
            data_volume_id=data_volume_id,
            volume_names=SCALEWAY_DATA_VOLUME_NAMES,
            user_data=user_data,
            dist_dir=Path(dist_dir) if dist_dir else None,
            start_timeout=timedelta(minutes=start_timeout_minutes),
            max_duration=timedelta(hours=max_duration_hours),
        )

        execute_remote_run(
            cfg,
            provider=ProviderAvecRetry(provider),
            trigger=_make_trigger(deployment, env),
            get_state=_get_state,
            cancel=_cancel,
            sync_dist=_make_sync_dist(),
            fetch_boot_log=_make_fetch_boot_log(provider, launcher_run_id),
            now=_now,
            sleep=_sleep,
            logger=logger,
            secret_strings=[v for v in env_vars.values() if v],
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sleep(secondes: float) -> None:
    time.sleep(secondes)


run_on_scaleway.on_failure(notify_exception_by_email)

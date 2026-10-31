"""Lanceur du run distant sur l'instance éphémère : fonction pure.

Aucune dépendance à Prefect ni au réseau ici : provider, déclenchement,
horloge, sommeil et journal sont injectés. Le user-data n'est jamais journalisé.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from logging import Logger
from pathlib import Path
from typing import Callable, Protocol

from src.infra.scaleway import Instance
from src.tasks.output import swap_dist


class Provider(Protocol):
    """Contrat minimal attendu d'un fournisseur d'instances (Task 3)."""

    def create_instance(
        self,
        *,
        name: str,
        instance_type: str,
        data_volume_id: str,
        user_data: str,
        tags: list[str],
    ) -> Instance: ...

    def delete_instance(self, instance_id: str, data_volume_id: str) -> None: ...

    def list_instances(self, tags: list[str]) -> list[Instance]: ...

    def volume_id(self, name: str) -> str: ...


class RunState(Enum):
    """États d'un run distant ; PENDING = planifié ou en attente de worker."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RemoteRunError(Exception):
    """Échec du run distant ; le message ne contient jamais le user-data ni de secret."""


@dataclass
class RemoteRunConfig:
    """Paramètres d'un run distant (le user-data n'est jamais journalisé)."""

    env: str
    deployment: str
    launcher_run_id: str
    instance_type: str
    data_volume_id: str
    volume_names: dict[str, str]
    user_data: str
    dist_dir: Path | None
    start_timeout: timedelta
    max_duration: timedelta
    poll_interval: timedelta = timedelta(seconds=30)


def _env_depuis_tags(tags: list[str]) -> str:
    """Extrait la valeur du tag `env:<valeur>`, chaîne vide si absent."""
    for tag in tags:
        if tag.startswith("env:"):
            return tag[len("env:") :]
    return ""


def cleanup_orphans(
    provider: Provider,
    env: str,
    now: datetime,
    max_age: timedelta,
    volume_names: dict[str, str],
) -> list[str]:
    """Détruit les instances orphelines, avant toute création.

    Deux règles : (1) toute instance de l'environnement courant ; (2) toute
    instance de n'importe quel environnement plus vieille que `max_age`. Le
    volume à détacher est résolu par nom via `provider.volume_id` : le
    mapping `volume_names` (env → nom de volume, ex. SCALEWAY_DATA_VOLUME_NAMES)
    doit couvrir tous les environnements du projet, sinon l'instance serait
    détruite sans détacher son volume. Un env absent du mapping est donc
    ignoré : `delete_instance` exige un vrai volume, jamais une chaîne vide.
    Retourne les ids détruits.
    """
    detruits: list[str] = []
    for instance in provider.list_instances(["decp-processing"]):
        env_instance = _env_depuis_tags(instance.tags)
        trop_ancienne = (now - instance.created_at) > max_age
        if not (env_instance == env or trop_ancienne):
            continue
        nom_volume = volume_names.get(env_instance)
        if nom_volume is None:
            continue
        provider.delete_instance(instance.id, provider.volume_id(nom_volume))
        detruits.append(instance.id)
    return detruits


def _masque(texte: str, secret_strings: list[str] | None) -> str:
    """Remplace chaque chaîne secrète par *** ; le user-data ne doit jamais fuir."""
    for secret in secret_strings or []:
        if secret:
            texte = texte.replace(secret, "***")
    return texte


def _attendre_fin(
    cfg: RemoteRunConfig,
    run_id: str,
    *,
    get_state: Callable[[str], RunState],
    cancel: Callable[[str], None],
    fetch_boot_log: Callable[[str], str],
    now: Callable[[], datetime],
    sleep: Callable[[float], None],
    logger: Logger,
    secret_strings: list[str] | None,
) -> None:
    """Attend la fin du run, en bornant chaque délai par son état.

    Le délai de démarrage ne coure que pendant PENDING : il est réinitialisé à
    chaque retour en PENDING, seul un PENDING continu peut le dépasser. La
    durée maximale ne court que pendant RUNNING, depuis le premier RUNNING vu.
    Le boot log peut échoer le user-data : il est masqué par `secret_strings`.
    """
    depuis_pending: datetime | None = None
    premier_running: datetime | None = None
    while True:
        etat = get_state(run_id)
        if etat is RunState.COMPLETED:
            return
        if etat is RunState.FAILED:
            logger.error(f"Le run distant {run_id} a échoué.")
            raise RemoteRunError(f"Le run distant {run_id} a échoué.")
        if etat is RunState.PENDING:
            if depuis_pending is None:
                depuis_pending = now()
            elif now() - depuis_pending > cfg.start_timeout:
                cancel(run_id)
                try:
                    boot_log = _masque(fetch_boot_log(run_id), secret_strings)
                except Exception as exc:
                    logger.error(f"Boot log indisponible pour le run {run_id} : {exc}")
                else:
                    logger.error(
                        f"Log de démarrage de l'instance pour le run {run_id} :\n{boot_log}"
                    )
                message = f"Délai de démarrage dépassé ({cfg.start_timeout}) pour le run {run_id}."
                logger.error(message)
                raise RemoteRunError(message)
        elif etat is RunState.RUNNING:
            depuis_pending = None
            if premier_running is None:
                premier_running = now()
            elif now() - premier_running > cfg.max_duration:
                cancel(run_id)
                message = f"Durée maximale dépassée ({cfg.max_duration}) pour le run {run_id}."
                logger.error(message)
                raise RemoteRunError(message)
        sleep(cfg.poll_interval.total_seconds())


def _rapatrier(
    dist_dir: Path, ip: str, sync_dist: Callable[[str, Path], None], logger: Logger
) -> None:
    """Rapatrie les données distantes puis bascule atomiquement `dist_dir`."""
    wip = Path(f"{dist_dir}_wip")
    try:
        sync_dist(ip, wip)
        swap_dist(wip, dist_dir)
    except Exception as exc:
        raise RemoteRunError(
            f"Échec du rapatriement des données vers {dist_dir}."
        ) from exc
    logger.info(f"Données rapatriées et publiées dans {dist_dir}")


def execute_remote_run(
    cfg: RemoteRunConfig,
    *,
    provider: Provider,
    trigger: Callable[[], str],
    get_state: Callable[[str], RunState],
    cancel: Callable[[str], None],
    sync_dist: Callable[[str, Path], None],
    fetch_boot_log: Callable[[str], str],
    now: Callable[[], datetime],
    sleep: Callable[[float], None],
    logger: Logger,
    secret_strings: list[str] | None = None,
) -> Instance:
    """Exécute un run sur une instance éphémère, de la création à la destruction.

    Cycle : nettoyage des orphelines → création de l'instance → déclenchement
    du run → attente de la fin → rapatriement et bascule de `dist_dir` si
    demandé → destruction de l'instance dans tous les cas. Retourne l'instance
    créée ; lève `RemoteRunError` en cas d'échec, avec un message qui ne
    contient jamais le user-data ni de secret. `secret_strings` (construit par
    l'appelant depuis le user-data et les valeurs de env_vars) est masqué
    dans les textes externes journalisés, comme le boot log.
    """
    orphelins = cleanup_orphans(
        provider, cfg.env, now(), cfg.max_duration, cfg.volume_names
    )
    if orphelins:
        logger.info(f"Instances orphelines détruites avant création : {orphelins}")

    instance = provider.create_instance(
        name=f"decp-processing-{cfg.env}-{cfg.launcher_run_id}",
        instance_type=cfg.instance_type,
        data_volume_id=cfg.data_volume_id,
        user_data=cfg.user_data,
        tags=["decp-processing", f"env:{cfg.env}", f"flow-run:{cfg.launcher_run_id}"],
    )
    logger.info(f"Instance {instance.id} créée ({instance.ip})")

    try:
        run_id = trigger()
        logger.info(f"Run {run_id} déclenché sur l'instance {instance.id}")
        _attendre_fin(
            cfg,
            run_id,
            get_state=get_state,
            cancel=cancel,
            fetch_boot_log=fetch_boot_log,
            now=now,
            sleep=sleep,
            logger=logger,
            secret_strings=secret_strings,
        )
        if cfg.dist_dir is not None:
            if instance.ip is None:
                raise RemoteRunError(
                    f"L'instance {instance.id} n'a pas d'IP publique, "
                    "rapatriement des données impossible."
                )
            _rapatrier(cfg.dist_dir, instance.ip, sync_dist, logger)
        return instance
    finally:
        provider.delete_instance(instance.id, cfg.data_volume_id)
        logger.info(f"Instance {instance.id} détruite")

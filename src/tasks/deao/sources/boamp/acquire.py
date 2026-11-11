"""Acquisition des jours BOAMP : plan, traitement d'un jour, lots (issue #178)."""

import shutil
from collections import Counter
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path

from prefect import task

from src.config import (
    BOAMP_RECRAWL_DAYS,
    DEAO_PARTITIONS_DIR,
    MAX_PREFECT_WORKERS,
)
from src.tasks.deao.sources.boamp import fetch
from src.tasks.deao.sources.boamp.archive import (
    list_archived_days,
    read_day_archive,
    write_day_archive,
)
from src.tasks.deao.sources.boamp.parse import parse_day
from src.tasks.utils import get_logger, run_in_pool

BATCH_DAYS = 30


class Action(StrEnum):
    FROM_ARCHIVE = "from_archive"
    CRAWL = "crawl"
    RECRAWL = "recrawl"


def plan_days(
    start: date,
    end: date,
    recrawl_days: int,
    local_days: set[date],
    archived_days: set[date],
) -> list[tuple[date, Action]]:
    plan = []
    d = start
    while d <= end:
        if d > end - timedelta(days=recrawl_days):
            plan.append((d, Action.RECRAWL))
        elif d in local_days:
            pass
        elif d in archived_days:
            plan.append((d, Action.FROM_ARCHIVE))
        else:
            plan.append((d, Action.CRAWL))
        d += timedelta(days=1)
    return plan


def partition_path(d: date, source: str = "boamp") -> Path:
    return DEAO_PARTITIONS_DIR / source / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.parquet"


def local_days(source: str = "boamp") -> set[date]:
    """Jours ayant une partition `<source>/YYYY/MM/DD.parquet` ; le reste est ignoré."""
    days = set()
    for path in (DEAO_PARTITIONS_DIR / source).glob("*/*/*.parquet"):
        try:
            days.add(
                date(
                    int(path.parent.parent.name), int(path.parent.name), int(path.stem)
                )
            )
        except ValueError:
            continue
    return days


def _crawl(d: date, known: dict[str, bytes]) -> dict[str, bytes]:
    """Fichiers du jour absents de `known`, téléchargés (lève au moindre échec)."""
    base_url, names = fetch.list_day_files(d)
    return {
        name: fetch.download_xml(base_url, name) for name in names if name not in known
    }


def process_day(d: date, action: Action, archived: bool) -> Counter:
    """Acquiert un jour et écrit sa partition.

    Tout est gardé en mémoire : une erreur de listing ou de téléchargement lève
    avant toute écriture. L'archive brute est écrite avant le parsing, pour
    qu'un échec de parsing ne la prive pas des fichiers téléchargés ; la
    partition n'est écrite qu'après un parsing réussi.
    """
    if action == Action.FROM_ARCHIVE:
        files = read_day_archive(d)
        write_archive = False
    elif action == Action.CRAWL:
        files = _crawl(d, {})
        write_archive = True
    else:
        files = read_day_archive(d) if archived else {}
        new = _crawl(d, files)
        files = {**files, **new}
        write_archive = bool(new) or not archived

    if write_archive:
        write_day_archive(d, files)

    df, compteur = parse_day(files)
    if not files:
        compteur["jour_vide"] += 1

    path = partition_path(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(path)
    compteur["jour_traite"] += 1
    return compteur


def _process_day_args(args: tuple[date, Action, bool]) -> Counter:
    return process_day(*args)


def process_batch(items: list[tuple[date, Action, bool]]) -> Counter:
    """Traite un lot de jours en parallèle ; compte les jours en échec."""
    results = run_in_pool(
        _process_day_args,
        items,
        max_workers=MAX_PREFECT_WORKERS,
        logger=get_logger("INFO"),
        label=lambda item: f"{item[0]} ({item[1]})",
    )
    total: Counter = Counter()
    for _, compteur in results:
        total.update(compteur)
    total["jour_en_erreur"] += len(items) - len(results)
    return total


@task(retries=2)
def process_batch_task(items: list[tuple[date, Action, bool]]) -> Counter:
    return process_batch(items)


def acquire_boamp(start: date, end: date, rebuild: bool) -> Counter:
    if rebuild:
        shutil.rmtree(DEAO_PARTITIONS_DIR / "boamp", ignore_errors=True)
    archived_days = list_archived_days()
    plan = plan_days(start, end, BOAMP_RECRAWL_DAYS, local_days(), archived_days)
    items = [(d, action, d in archived_days) for d, action in plan]
    total: Counter = Counter()
    for i in range(0, len(items), BATCH_DAYS):
        total.update(process_batch_task(items[i : i + BATCH_DAYS]))
    return total

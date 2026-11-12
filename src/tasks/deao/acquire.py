"""Acquisition par jour d'une source : plan, traitement d'un jour, lots (issue #178)."""

import shutil
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path

import polars as pl

from src.config import BOAMP_RECRAWL_DAYS, DEAO_PARTITIONS_DIR, LOG_LEVEL
from src.tasks.deao.archive import (
    list_archived_days,
    read_day_archive,
    write_day_archive,
)
from src.tasks.utils import get_logger, run_in_pool

BATCH_DAYS = 30


@dataclass(frozen=True)
class Source:
    nom: str  # "boamp", "joue"
    s3_prefix: str
    list_day_files: Callable[[date], tuple[str | None, list[str]]]  # (base, noms)
    download: Callable[[str | None, str], bytes]  # (base, nom) -> XML
    parse_day: Callable[[dict[str, bytes]], tuple[pl.DataFrame, Counter]]
    max_workers: int


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


def partition_path(source: str, d: date) -> Path:
    return DEAO_PARTITIONS_DIR / source / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.parquet"


def local_days(source: str) -> set[date]:
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


def _crawl(source: Source, d: date, known: dict[str, bytes]) -> dict[str, bytes]:
    """Fichiers du jour absents de `known`, téléchargés (lève au moindre échec)."""
    base, names = source.list_day_files(d)
    return {name: source.download(base, name) for name in names if name not in known}


def process_day(source: Source, d: date, action: Action, archived: bool) -> Counter:
    """Acquiert un jour et écrit sa partition.

    Tout est gardé en mémoire : une erreur de listing ou de téléchargement lève
    avant toute écriture. L'archive brute est écrite avant le parsing, pour
    qu'un échec de parsing ne la prive pas des fichiers téléchargés ; la
    partition n'est écrite qu'après un parsing réussi.
    """
    if action == Action.FROM_ARCHIVE:
        files = read_day_archive(source.s3_prefix, d)
        write_archive = False
    elif action == Action.CRAWL:
        files = _crawl(source, d, {})
        write_archive = True
    else:
        files = read_day_archive(source.s3_prefix, d) if archived else {}
        new = _crawl(source, d, files)
        files = {**files, **new}
        write_archive = bool(new) or not archived

    if write_archive:
        write_day_archive(source.s3_prefix, d, files)

    df, compteur = source.parse_day(files)
    if not files:
        compteur["jour_vide"] += 1

    path = partition_path(source.nom, d)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(path)
    compteur["jour_traite"] += 1
    return compteur


def process_batch(source: Source, items: list[tuple[date, Action, bool]]) -> Counter:
    """Traite un lot de jours en parallèle ; compte les jours en échec."""
    logger = get_logger(level=LOG_LEVEL)
    logger.info(
        f"Process batch, première date : {items[0][0].strftime(format='YYYY-MM-dd')}"
    )

    def process_item(item: tuple[date, Action, bool]) -> Counter:
        return process_day(source, *item)

    results = run_in_pool(
        process_item,
        items,
        max_workers=source.max_workers,
        logger=get_logger("INFO"),
        label=lambda item: f"{source.nom} {item[0]} ({item[1]})",
    )
    total: Counter = Counter()
    for _, compteur in results:
        total.update(compteur)
    total["jour_en_erreur"] += len(items) - len(results)
    return total


def acquire(source: Source, start: date, end: date, rebuild: bool) -> Counter:
    if rebuild:
        shutil.rmtree(DEAO_PARTITIONS_DIR / source.nom, ignore_errors=True)
    archived_days = list_archived_days(source.s3_prefix)
    plan = plan_days(
        start, end, BOAMP_RECRAWL_DAYS, local_days(source.nom), archived_days
    )
    items = [(d, action, d in archived_days) for d, action in plan]
    total: Counter = Counter()
    for i in range(0, len(items), BATCH_DAYS):
        total.update(process_batch(source, items[i : i + BATCH_DAYS]))
    return total

"""Flow boamp_processing : acquisition + aplatissement + publication (issue #174)."""

from datetime import date, timedelta
from pathlib import Path

from lxml import etree
from prefect import flow, task

from src.config import (
    BOAMP_BACKFILL_YEARS,
    BOAMP_DATASET_ID,
    BOAMP_PARTITIONS_DIR,
    BOAMP_RECRAWL_DAYS,
    BOAMP_START_DATE,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    LOG_LEVEL,
    MAX_PREFECT_WORKERS,
)
from src.tasks.boamp.fetch import (
    download_xml,
    iter_archive_xml,
    iter_days,
    list_day_files,
)
from src.tasks.boamp.flatten import annonce_to_rows, concat_partitions, rows_to_frame
from src.tasks.output import sink_to_files
from src.tasks.publish import publish_boamp_to_datagouv
from src.tasks.utils import get_logger, print_all_config, run_in_pool


def _partition_path(d: date) -> Path:
    return (
        BOAMP_PARTITIONS_DIR / f"{d.year}" / f"{d.month:02d}" / f"{d.day:02d}.parquet"
    )


def _rows_from_bytes(xml_bytes: bytes, source_name: str) -> list[dict]:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return []
    return annonce_to_rows(root, source_name)


@task(retries=2, timeout_seconds=1800)
def process_day(d: date) -> Path:
    """Télécharge, aplatit et écrit la partition parquet d'un jour."""
    logger = get_logger(level=LOG_LEVEL)
    rows: list[dict] = []
    for filename in list_day_files(d):
        rows.extend(_rows_from_bytes(download_xml(d, filename), filename))

    part = _partition_path(d)
    part.parent.mkdir(parents=True, exist_ok=True)
    rows_to_frame(rows).write_parquet(part)
    logger.info(f"📦 {d.isoformat()} : {len(rows)} lignes -> {part.name}")
    return part


@task(retries=1, timeout_seconds=7200)
def backfill_year(year: int) -> Path:
    """Aplatit toutes les archives tar.gz d'une année dans un parquet annuel."""
    logger = get_logger(level=LOG_LEVEL)
    rows: list[dict] = []
    for name, xml_bytes in iter_archive_xml(year):
        rows.extend(_rows_from_bytes(xml_bytes, Path(name).name))
    out = BOAMP_PARTITIONS_DIR / f"backfill_{year}.parquet"
    rows_to_frame(rows).write_parquet(out)
    logger.info(f"🗄️ Backfill {year} : {len(rows)} lignes -> {out.name}")
    return out


@flow(log_prints=True)
def boamp_processing():
    logger = get_logger(level=LOG_LEVEL)
    logger.info("🚀  Début du flow boamp_processing")
    print_all_config()

    # 1. Backfill des années complètes (une fois, via marqueur).
    for year in BOAMP_BACKFILL_YEARS:
        marker = BOAMP_PARTITIONS_DIR / f".backfill_{year}.done"
        if marker.exists():
            continue
        backfill_year(year)
        marker.touch()

    # 2. Incrémental sur la période courante (crawl quotidien).
    crawl_start = date(max(BOAMP_BACKFILL_YEARS) + 1, 1, 1)
    start = max(date.fromisoformat(BOAMP_START_DATE), crawl_start)
    end = date.today()
    recrawl_floor = end - timedelta(days=BOAMP_RECRAWL_DAYS)
    days_to_process = [
        d
        for d in iter_days(start, end)
        if not (_partition_path(d).exists() and d < recrawl_floor)
    ]
    run_in_pool(
        process_day,
        days_to_process,
        max_workers=MAX_PREFECT_WORKERS,
        logger=logger,
        label=lambda d: f"jour {d.isoformat()}",
    )

    # 3. Concaténation -> écriture -> publication.
    logger.info("Concaténation des partitions BOAMP...")
    lf = concat_partitions(BOAMP_PARTITIONS_DIR)
    sink_to_files(lf, DIST_DIR / "boamp")

    if DECP_PROCESSING_PUBLISH and BOAMP_DATASET_ID:
        logger.info("Publication BOAMP sur data.gouv.fr...")
        publish_boamp_to_datagouv()
    else:
        logger.info(
            "Publication BOAMP désactivée (DECP_PROCESSING_PUBLISH/BOAMP_DATASET_ID)."
        )

    logger.info("☑️  Fin du flow boamp_processing.")

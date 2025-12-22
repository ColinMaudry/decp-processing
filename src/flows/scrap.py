from pathlib import Path
from shutil import rmtree

from prefect import flow

from src.config import (
    DATE_NOW,
    DIST_DIR,
    LOG_LEVEL,
    MONTH_NOW,
    SCRAPING_MODE,
    SCRAPING_TARGET,
)
from src.tasks.scrap import scrap_aws_month, scrap_marches_securises_month
from src.tasks.utils import get_logger


@flow(log_prints=True)
def scrap(target: str = None, mode: str = None, month=None, year=None):
    logger = get_logger(level=LOG_LEVEL)
    # Remise à zéro du dossier dist
    dist_dir: Path = DIST_DIR / target
    if dist_dir.exists():
        logger.debug(f"Suppression de {dist_dir}...")
        rmtree(dist_dir)
    else:
        dist_dir.mkdir(parents=True)

    # Sélection du target
    target = target or SCRAPING_TARGET

    # Sélection de la fonction de scraping en fonction de target
    if target == "aws":
        scrap_target_month = scrap_aws_month
    elif target == "marches-securises.fr":
        scrap_target_month = scrap_marches_securises_month
    else:
        logger.error("Quel target ?")
        raise ValueError

    current_year = DATE_NOW[:4]
    current_month = MONTH_NOW[-2:]
    month = month or current_month
    year = year or current_year

    # Sélection du mode
    mode = mode or SCRAPING_MODE

    # Sélection de la plage temporelle
    if mode == "month":
        scrap_target_month(year, month, dist_dir)

    elif mode == "year":
        for month in reversed(range(1, 13)):
            month = str(month).zfill(2)
            scrap_target_month(year, month, dist_dir)

    elif mode == "all":
        current_year = int(current_year)
        for year in reversed(range(2018, current_year + 2)):
            scrap(target=target, mode="year", year=str(year))

    else:
        logger.error("Mauvaise configuration")

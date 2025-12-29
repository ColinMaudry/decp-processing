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
from src.tasks.utils import get_logger, print_all_config
from tasks.scrap.aws import scrap_aws_month
from tasks.scrap.dume import scrap_dume_month
from tasks.scrap.klekoon import scrap_klekoon
from tasks.scrap.marches_securises import scrap_marches_securises_month


@flow(log_prints=True)
def scrap(target: str, mode: str, month=None, year=None):
    logger = get_logger(level=LOG_LEVEL)

    print_all_config()

    # Remise à zéro du dossier dist
    dist_dir: Path = DIST_DIR / target
    if dist_dir.exists():
        logger.info(f"Suppression de {dist_dir}...")
        rmtree(dist_dir)

    dist_dir.mkdir(parents=True, exist_ok=True)

    # Sélection de la fonction de scraping en fonction de target
    if target == "aws":
        scrap_target_month = scrap_aws_month
    elif target == "marches-securises.fr":
        scrap_target_month = scrap_marches_securises_month
    elif target == "dume":
        scrap_target_month = scrap_dume_month
    elif target == "klekoon":
        # Klekoon présent ses données par acheteur et non de manière temporelle
        # donc on télécharge tout à chaque fois
        scrap_klekoon(dist_dir)
        return
    else:
        logger.error("Quel target ?")
        raise ValueError

    current_year = DATE_NOW[:4]
    current_month = MONTH_NOW[-2:]
    month = month or current_month
    year = year or current_year

    # Récapitulatif de la config
    # mode et target doivent être passés en paramètre
    # les éventuelles env sont injectées via /run_flow.py
    # en prod les paramètres sont spécifiées dans le deployment Prefect
    logger.info(f"""
    Target: {target} (env {SCRAPING_TARGET})
    Mode: {mode} (env {SCRAPING_MODE})
    Year: {year}
    Month: {month}""")

    # Sélection de la plage temporelle
    if mode == "month":
        scrap_target_month(year, month, dist_dir)

    elif mode == "year":
        for month in reversed(range(1, 13)):
            month = str(month).zfill(2)
            scrap_target_month(year, month, dist_dir)

    elif mode == "all":
        current_year = int(current_year)
        for year in reversed(range(2018, current_year + 1)):
            scrap(target=target, mode="year", year=str(year))

    else:
        logger.error("Mauvaise configuration")

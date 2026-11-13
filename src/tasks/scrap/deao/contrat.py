"""Contrat des scrapers de plateformes DEAO (sources fermées, dataset b)."""

from collections.abc import Callable
from datetime import date

import polars as pl

from src.tasks.scrap.deao.atexo.scraper import scraper_atexo

# Un scraper reçoit la date du run et renvoie les lignes au schéma
# `published_schema()`, avec `sourceDataset` = code de la source du registre.
# Il peut lever `src.tasks.scrap.deao.erreurs.ScrapPartiel` (lignes publiées, mais
# source comptée en erreur par scrap_closed).
Scraper = Callable[[date], pl.DataFrame]

# Un scraper s'y ajoute par son `code` de registre. Construction paresseuse : aucun
# accès au registre, au réseau ni à S3 à l'import.
SCRAPERS: dict[str, Scraper] = {
    code: scraper_atexo(code) for code in ("megalis_bretagne", "maximilien")
}

"""Contrat des scrapers de plateformes DEAO (sources fermées, dataset b)."""

from collections.abc import Callable
from datetime import date

import polars as pl

# Un scraper reçoit la date du run et renvoie les lignes au schéma
# `published_schema()`, avec `sourceDataset` = code de la source du registre.
Scraper = Callable[[date], pl.DataFrame]

# Vide en v1 : un futur scraper s'y ajoute par son `code` de registre.
SCRAPERS: dict[str, Scraper] = {}

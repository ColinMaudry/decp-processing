"""Contrat des scrapers de plateformes DEAO (sources fermées, dataset b)."""

from collections.abc import Callable
from datetime import date

import polars as pl

from src import config
from src.tasks.scrap.deao.atexo.scraper import scraper_atexo
from src.tasks.scrap.deao.aws.scraper import scraper_aws

# Un scraper reçoit la date du run et renvoie les lignes au schéma
# `published_schema()`, avec `sourceDataset` = code de la source du registre.
# Il peut lever `src.tasks.scrap.deao.erreurs.ScrapPartiel` (lignes publiées, mais
# source comptée en erreur par scrap_closed).
Scraper = Callable[[date], pl.DataFrame]


def scrapers_du_registre(registre: tuple[config.SourceDeao, ...]) -> dict[str, Scraper]:
    """Un scraper par source du registre dont la `plateforme` est connue.

    Construction paresseuse : aucun accès au réseau ni à S3 avant l'appel."""
    fabriques = {"atexo": scraper_atexo, "aws": scraper_aws}
    return {
        e.code: fabriques[e.plateforme](e.code)
        for e in registre
        if e.plateforme in fabriques
    }


SCRAPERS: dict[str, Scraper] = scrapers_du_registre(config.DEAO_REGISTRE)

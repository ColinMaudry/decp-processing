# ruff: noqa: F811
import importlib

import polars as pl
import pytest

from src import config
from src.config import SourceDeao
from src.tasks.deao.schema import published_schema
from tests.deao_fakes import fake_datagouv  # noqa: F401

flow_module = importlib.import_module("src.flows.scrap_closed")

REGISTRE = (
    SourceDeao("plateforme_a", "A", False, "res-a", 1),
    SourceDeao("plateforme_b", "B", False, "res-b", 1, skip=True),
    SourceDeao("boamp", "BOAMP", True, "res-boamp", 1),
)


def _lignes(code, n=2):
    df = pl.DataFrame(
        {c: [None] * n for c in published_schema()}, schema=published_schema()
    )
    return df.with_columns(pl.lit(code).alias("sourceDataset"))


@pytest.fixture
def env(monkeypatch, tmp_path, fake_datagouv):
    monkeypatch.setattr(config, "DEAO_REGISTRE", REGISTRE)
    monkeypatch.setattr(config, "DEAO_DIST_DIR", tmp_path)
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    appels = []
    monkeypatch.setattr(
        flow_module, "deao_processing", lambda run_date=None: appels.append(run_date)
    )
    executes = []

    def scraper(code):
        def f(jour):
            executes.append(code)
            return _lignes(code)

        return f

    scrapers = {c: scraper(c) for c in ("plateforme_a", "plateforme_b")}
    monkeypatch.setattr(flow_module, "SCRAPERS", scrapers)
    return fake_datagouv, appels, executes, scrapers


def test_sans_scraper_lance_traitement(env, monkeypatch):
    publication, appels, executes, _ = env
    monkeypatch.setattr(config, "DEAO_REGISTRE", (REGISTRE[2],))
    flow_module.scrap_closed(run_date="2026-10-15")
    assert appels == ["2026-10-15"]
    assert publication.publies == []
    assert executes == []


def test_deux_scrapers_dont_un_en_erreur(env, monkeypatch):
    publication, appels, _, scrapers = env
    monkeypatch.setattr(
        config,
        "DEAO_REGISTRE",
        (REGISTRE[0], SourceDeao("plateforme_b", "B", False, "res-b", 1)),
    )

    def casse(jour):
        raise ValueError("boum")

    scrapers["plateforme_b"] = casse
    with pytest.raises(RuntimeError, match="plateforme_b"):
        flow_module.scrap_closed(run_date="2026-10-15")
    assert [p[1] for p in publication.publies] == ["res-a"]
    assert appels == ["2026-10-15"]


def test_solo_seulement_les_listes(env, monkeypatch):
    publication, appels, executes, _ = env
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", ["", "plateforme_b"])
    flow_module.scrap_closed()
    assert executes == ["plateforme_b"]
    assert publication.publies == []
    assert appels == [None]


def test_scraper_schema_non_conforme(env, monkeypatch):
    publication, appels, _, scrapers = env
    scrapers["plateforme_a"] = lambda jour: _lignes("plateforme_a").drop(
        "sourceDataset"
    )
    with pytest.raises(RuntimeError, match="plateforme_a"):
        flow_module.scrap_closed()
    assert publication.publies == []
    assert appels == [None]


def test_code_sans_scraper(env, monkeypatch):
    _, appels, _, scrapers = env
    del scrapers["plateforme_a"]
    with pytest.raises(RuntimeError, match="plateforme_a"):
        flow_module.scrap_closed()
    assert appels == [None]


def test_sous_minimum_non_publie(env, monkeypatch):
    publication, appels, _, _ = env
    monkeypatch.setattr(
        config, "DEAO_REGISTRE", (SourceDeao("plateforme_a", "A", False, "res-a", 5),)
    )
    with pytest.raises(RuntimeError, match="plateforme_a"):
        flow_module.scrap_closed()
    assert publication.publies == []
    assert appels == [None]


def test_echec_publication_d_un_scraper_non_bloquant(env, monkeypatch):
    publication, appels, _, _ = env
    monkeypatch.setattr(
        config,
        "DEAO_REGISTRE",
        (REGISTRE[0], SourceDeao("plateforme_b", "B", False, "res-b", 1)),
    )
    publication.echecs_publication.add("res-a")
    with pytest.raises(RuntimeError, match="plateforme_a"):
        flow_module.scrap_closed(run_date="2026-10-15")
    assert [p[1] for p in publication.publies] == ["res-a", "res-b"]
    assert appels == ["2026-10-15"]


def test_scraper_avec_autre_source_dataset_non_publie(env):
    publication, appels, _, scrapers = env
    scrapers["plateforme_a"] = lambda jour: pl.concat(
        [_lignes("plateforme_a"), _lignes("boamp", 1)]
    )
    with pytest.raises(RuntimeError, match="plateforme_a"):
        flow_module.scrap_closed()
    assert publication.publies == []
    assert appels == [None]


def test_scrapers_atexo_enregistres():
    from src.tasks.scrap.deao.contrat import SCRAPERS

    assert {"megalis_bretagne", "maximilien"} <= set(SCRAPERS)


def test_scraper_execute_dans_le_contexte_du_flow(env, monkeypatch):
    """Les scrapers tournent dans des fils : ils doivent garder le logger du run."""
    from prefect import get_run_logger

    publication, _, _, scrapers = env

    def avec_logger(jour):
        get_run_logger().info("journal du scraper")
        return _lignes("plateforme_a")

    scrapers["plateforme_a"] = avec_logger
    flow_module.scrap_closed(run_date="2026-10-15")
    assert [p[1] for p in publication.publies] == ["res-a"]

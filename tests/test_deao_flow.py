# ruff: noqa: F811
import importlib
import re
from datetime import date
from pathlib import Path

import httpx
import polars as pl
import pytest

from src.tasks.deao import acquire
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA
from src.tasks.deao.schema import AVIS_LOT_SCHEMA, PUBLISHED_FIELDS, conform_avis
from src.tasks.deao.sources.boamp import fetch
from tests.deao_fakes import fake_s3  # noqa: F401
from tests.test_deao_enrich import COG, _lf_etab, _lf_ul

flow_module = importlib.import_module("src.flows.deao_processing")

DATA = Path(__file__).parent / "data" / "deao"
_DATE_RE = re.compile(rb"<DATE_PUBLICATION>(\d{4}-\d{2}-\d{2})")


def _fixtures_par_jour() -> dict[date, dict[str, bytes]]:
    jours: dict[date, dict[str, bytes]] = {}
    for path in sorted(DATA.glob("*.xml")):
        xml = path.read_bytes()
        d = date.fromisoformat(_DATE_RE.search(xml).group(1).decode())
        jours.setdefault(d, {})[path.name] = xml
    return jours


@pytest.fixture
def dila(monkeypatch):
    """Faux DILA : index du jour (flux courant) et XML des fixtures ; 404 sinon."""
    pages: dict[str, tuple[int, str | bytes]] = {}
    for d, files in _fixtures_par_jour().items():
        index_url = fetch.day_index_urls(d)[0]
        pages[index_url] = (
            200,
            "".join(f'<a href="{name}">{name}</a>\n' for name in files),
        )
        for name, xml in files.items():
            pages[index_url + name] = (200, xml)
    calls: list[str] = []

    def http_get(url):
        calls.append(url)
        status, body = pages.get(url, (404, ""))
        if isinstance(body, bytes):
            return httpx.Response(status, content=body)
        return httpx.Response(status, text=body)

    monkeypatch.setattr(fetch, "http_get", http_get)
    return calls


@pytest.fixture
def flow_env(monkeypatch, tmp_path, fake_s3, dila):  # noqa: F811
    partitions = tmp_path / "avis"
    dist = tmp_path / "dist"
    monkeypatch.setattr(acquire, "DEAO_PARTITIONS_DIR", partitions)
    # Valeur de la spec, indépendante d'un éventuel .env local
    monkeypatch.setattr(acquire, "BOAMP_RECRAWL_DAYS", 3)
    monkeypatch.setattr(flow_module, "DEAO_PARTITIONS_DIR", partitions)
    monkeypatch.setattr(flow_module, "DEAO_DIST_DIR", dist)
    monkeypatch.setattr(flow_module, "BOAMP_START_DATE", "2026-09-12")
    monkeypatch.setattr(flow_module, "DEAO_REBUILD", False)
    # SIRENE présent : pas de prétraitement déclenché
    monkeypatch.setattr(flow_module, "SIRENE_DATA_DIR", tmp_path)
    monkeypatch.setattr(
        flow_module,
        "sirene_preprocess",
        lambda: pytest.fail("sirene_preprocess ne doit pas être appelé"),
    )
    monkeypatch.setattr(
        flow_module, "charger_referentiels", lambda: (_lf_ul(), _lf_etab(), COG)
    )
    monkeypatch.setattr(
        flow_module,
        "geocoder_adresses",
        lambda adresses, today: pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA),
    )
    monkeypatch.setattr("src.config.DECP_PROCESSING_PUBLISH", False)
    return dist


def test_deao_processing_bout_en_bout(flow_env):
    dist = flow_env
    flow_module.deao_processing(run_date="2026-10-15")
    df = pl.read_parquet(dist / "deao.parquet")
    assert df.columns == [f.name for f in PUBLISHED_FIELDS]
    assert df.height == 31 and df["uid"].is_unique().all()
    assert (dist / "deao.csv.gz").exists() and (dist / "schema.json").exists()
    assert "avisLies" not in pl.read_csv(dist / "deao.csv").columns
    assert set(df["statut"]) >= {"ouvert", "annule", "pre-information"}


def test_deao_processing_second_run_ne_retelecharge_que_la_fenetre(flow_env, dila):
    flow_module.deao_processing(run_date="2026-10-15")
    dila.clear()
    flow_module.deao_processing(run_date="2026-10-15")
    fenetre = {date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)}
    index_fenetre = {url for d in fenetre for url in fetch.day_index_urls(d)}
    assert dila
    assert set(dila) <= index_fenetre
    assert {d for d in fenetre if fetch.day_index_urls(d)[0] in dila} == fenetre


def test_deao_processing_geocodage_indisponible_continue(flow_env, monkeypatch):
    def panne(adresses, today):
        raise httpx.ConnectError("Géoplateforme indisponible")

    monkeypatch.setattr(flow_module, "geocoder_adresses", panne)
    rapports = []
    monkeypatch.setattr(flow_module, "publier_rapport", rapports.append)
    flow_module.deao_processing(run_date="2026-10-15")
    assert pl.read_parquet(flow_env / "deao.parquet").height == 31
    lignes = {ligne["indicateur"]: ligne["valeur"] for ligne in rapports[0]}
    assert lignes["geocodage_en_erreur"] == 1


def test_deao_processing_jour_en_erreur_publie_puis_echoue(flow_env, monkeypatch):
    """R20 : les jours sains sont écrits (et publiés), puis le run échoue."""
    jour_ko = fetch.day_index_urls(date(2026, 10, 1))[0]
    http_get = fetch.http_get

    def http_get_avec_panne(url):
        if url == jour_ko:
            return httpx.Response(500, text="")
        return http_get(url)

    monkeypatch.setattr(fetch, "http_get", http_get_avec_panne)
    publications = []
    monkeypatch.setattr(flow_module, "should_publish_deao", lambda n: True)
    monkeypatch.setattr(flow_module, "publish_deao_to_datagouv", publications.append)
    with pytest.raises(RuntimeError, match="1 jour"):
        flow_module.deao_processing(run_date="2026-10-15")
    assert pl.read_parquet(flow_env / "deao.parquet").height == 31
    assert publications == [flow_env]


def test_lire_partitions_tolere_un_changement_de_contrat(tmp_path, monkeypatch):
    """Des partitions écrites avant un changement de contrat (colonne ajoutée ou
    retirée) n'arrêtent pas la lecture : colonne absente → null, en trop → ignorée."""
    monkeypatch.setattr(flow_module, "DEAO_PARTITIONS_DIR", tmp_path)
    mois = tmp_path / "boamp" / "2026" / "10"
    mois.mkdir(parents=True)
    ancienne = conform_avis(pl.DataFrame({"idAvis": ["26-0"]})).drop("sourceFile")
    ancienne.with_columns(pl.lit("x").alias("retiree")).write_parquet(
        mois / "01.parquet"
    )
    conform_avis(
        pl.DataFrame({"idAvis": ["26-1"], "sourceFile": ["https://x/26-1.xml"]})
    ).write_parquet(mois / "02.parquet")
    df = flow_module.lire_partitions()
    assert df.schema == pl.Schema(AVIS_LOT_SCHEMA)
    assert df.sort("idAvis")["sourceFile"].to_list() == [None, "https://x/26-1.xml"]


def test_deao_processing_lance_sirene_preprocess_si_absent(
    flow_env, monkeypatch, tmp_path
):
    appels = []
    monkeypatch.setattr(flow_module, "SIRENE_DATA_DIR", tmp_path / "sirene_absent")
    monkeypatch.setattr(flow_module, "sirene_preprocess", lambda: appels.append(1))
    flow_module.deao_processing(run_date="2026-10-15")
    assert appels == [1]


def test_deao_processing_source_file_flux_historique(flow_env):
    flow_module.deao_processing(run_date="2027-01-15")
    sources = pl.read_parquet(flow_env / "deao.parquet")["sourceFile"].drop_nulls()
    assert sources.len() > 0
    assert sources.str.contains("/FluxHistorique/Boamp_v240/2026/").all()

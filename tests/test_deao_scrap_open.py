# ruff: noqa: F811
import dataclasses
import importlib
import re
from datetime import date
from pathlib import Path

import httpx
import polars as pl
import pytest

from src import config
from src.flows.deao_processing import deao_processing as deao_processing_reel
from src.tasks.deao import acquire
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA
from src.tasks.deao.schema import (
    AVIS_LOT_SCHEMA,
    PUBLISHED_FIELDS,
    conform_avis,
    published_schema,
)
from src.tasks.scrap.deao.boamp_joue.boamp import fetch
from src.tasks.scrap.deao.boamp_joue.joue import identifiants as joue_identifiants
from tests.deao_fakes import fake_datagouv, fake_s3, fake_ted, no_sleep  # noqa: F401
from tests.test_deao_enrich import COG, _lf_etab, _lf_ul

flow_module = importlib.import_module("src.flows.scrap_open")

DATA = Path(__file__).parent / "data" / "deao"
RUN_DATE = "2026-10-15"  # lendemain de l'avis JOUE le plus récent des fixtures
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
def traitement(monkeypatch):
    """Espion à la place de deao_processing : liste des run_date reçus."""
    appels: list[str | None] = []
    monkeypatch.setattr(
        flow_module, "deao_processing", lambda run_date=None: appels.append(run_date)
    )
    return appels


@pytest.fixture
def flow_env(monkeypatch, tmp_path, fake_s3, dila, traitement):  # noqa: F811
    partitions = tmp_path / "avis"
    dist = tmp_path / "dist"
    monkeypatch.setattr(acquire, "DEAO_PARTITIONS_DIR", partitions)
    # Valeur de la spec, indépendante d'un éventuel .env local
    monkeypatch.setattr(acquire, "BOAMP_RECRAWL_DAYS", 3)
    monkeypatch.setattr(flow_module, "DEAO_PARTITIONS_DIR", partitions)
    monkeypatch.setattr(flow_module, "DEAO_DIST_DIR", dist)
    monkeypatch.setattr(flow_module, "BOAMP_START_DATE", "2026-09-12")
    monkeypatch.setattr(flow_module, "DEAO_REBUILD", False)
    # Les tests existants restent sans réseau TED ; ceux de JOUE ajoutent la source.
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ("boamp",))
    # Jamais de S3 réel pour la résolution des identifiants JOUE
    monkeypatch.setattr(joue_identifiants, "s3_client", lambda: fake_s3)
    monkeypatch.setattr(joue_identifiants, "DEAO_DATA_DIR", tmp_path)
    monkeypatch.setattr(joue_identifiants, "publish_to_s3", lambda *a, **k: None)
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


def test_scrap_open_bout_en_bout(flow_env):
    dist = flow_env
    flow_module.scrap_open(run_date="2026-10-15")
    df = pl.read_parquet(dist / "boamp.parquet")
    assert df.columns == [f.name for f in PUBLISHED_FIELDS]
    assert df.height == 31 and df["uid"].is_unique().all()
    assert not (dist / "deao.parquet").exists()
    assert set(df["statut"]) >= {"ouvert", "annule", "pre-information"}


def test_scrap_open_second_run_ne_retelecharge_que_la_fenetre(flow_env, dila):
    flow_module.scrap_open(run_date="2026-10-15")
    dila.clear()
    flow_module.scrap_open(run_date="2026-10-15")
    fenetre = {date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15)}
    index_fenetre = {url for d in fenetre for url in fetch.day_index_urls(d)}
    assert dila
    assert set(dila) <= index_fenetre
    assert {d for d in fenetre if fetch.day_index_urls(d)[0] in dila} == fenetre


def test_scrap_open_geocodage_indisponible_continue(flow_env, monkeypatch):
    def panne(adresses, today):
        raise httpx.ConnectError("Géoplateforme indisponible")

    monkeypatch.setattr(flow_module, "geocoder_adresses", panne)
    rapports = []
    monkeypatch.setattr(flow_module, "publier_rapport", rapports.append)
    flow_module.scrap_open(run_date="2026-10-15")
    assert pl.read_parquet(flow_env / "boamp.parquet").height == 31
    lignes = {ligne["indicateur"]: ligne["valeur"] for ligne in rapports[0]}
    assert lignes["geocodage_en_erreur"] == 1


@pytest.fixture
def publication(monkeypatch, fake_datagouv):  # noqa: F811
    monkeypatch.setattr("src.config.DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr("src.config.SOLO_DEAO_SCRAPS", [])
    return fake_datagouv


def _rid(code):
    return next(s.resource_id for s in config.DEAO_REGISTRE if s.code == code)


def test_scrap_open_jour_en_erreur_publie_puis_echoue(
    flow_env, publication, traitement, monkeypatch
):
    """R20 : les jours sains sont écrits et publiés, le traitement est lancé, puis le run échoue."""
    jour_ko = fetch.day_index_urls(date(2026, 10, 1))[0]
    http_get = fetch.http_get

    def http_get_avec_panne(url):
        if url == jour_ko:
            return httpx.Response(500, text="")
        return http_get(url)

    monkeypatch.setattr(fetch, "http_get", http_get_avec_panne)
    with pytest.raises(RuntimeError, match="1 jour"):
        flow_module.scrap_open(run_date="2026-10-15")
    assert pl.read_parquet(flow_env / "boamp.parquet").height == 31
    assert [p[:2] for p in publication.publies] == [("ds-b", _rid("boamp"))]
    assert traitement == ["2026-10-15"]


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
    df = flow_module.lire_partitions(("boamp",))
    assert df.schema == pl.Schema(AVIS_LOT_SCHEMA)
    assert df.sort("idAvis")["sourceFile"].to_list() == [None, "https://x/26-1.xml"]


def test_scrap_open_lance_sirene_preprocess_si_absent(flow_env, monkeypatch, tmp_path):
    appels = []
    monkeypatch.setattr(flow_module, "SIRENE_DATA_DIR", tmp_path / "sirene_absent")
    monkeypatch.setattr(flow_module, "sirene_preprocess", lambda: appels.append(1))
    flow_module.scrap_open(run_date="2026-10-15")
    assert appels == [1]


def test_scrap_open_source_file_flux_historique(flow_env):
    flow_module.scrap_open(run_date="2027-01-15")
    sources = pl.read_parquet(flow_env / "boamp.parquet")["sourceFile"].drop_nulls()
    assert sources.len() > 0
    assert sources.str.contains("/FluxHistorique/Boamp_v240/2026/").all()


_PUB_RE = re.compile(rb"<efbc:PublicationDate>(\d{4}-\d{2}-\d{2})")


@pytest.fixture
def ted_fixtures(fake_ted):  # noqa: F811
    """Faux TED servant les fixtures JOUE réelles à leur date de publication."""
    fake_ted.avis_par_jour.clear()
    fake_ted.identifiants.clear()
    for path in sorted((DATA / "joue").glob("*.xml")):
        xml = path.read_bytes()
        jour = date.fromisoformat(_PUB_RE.search(xml).group(1).decode())
        fake_ted.avis_par_jour.setdefault(jour, {})[path.stem] = xml
    return fake_ted


@pytest.fixture
def deux_sources(flow_env, monkeypatch, ted_fixtures):
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ("boamp", "joue"))
    monkeypatch.setattr(flow_module, "BOAMP_START_DATE", "2026-09-01")
    return ted_fixtures


def test_flow_sources_inconnue(flow_env, monkeypatch):
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ("boamp", "inconnue"))
    with pytest.raises(ValueError, match="inconnue"):
        flow_module.scrap_open(run_date=RUN_DATE)


def test_flow_sources_vide(flow_env, monkeypatch):
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ())
    with pytest.raises(ValueError, match="aucune source"):
        flow_module.scrap_open(run_date=RUN_DATE)


def test_liste_sources_dedupliquee_en_gardant_l_ordre():
    from src.config import parse_deao_sources

    assert parse_deao_sources("joue, boamp,joue,,boamp") == ("joue", "boamp")
    assert parse_deao_sources(" , ") == ()


def test_flow_deux_sources(flow_env, deux_sources):
    flow_module.scrap_open(run_date=RUN_DATE)
    df = pl.concat(
        [pl.read_parquet(flow_env / f"{c}.parquet") for c in ("boamp", "joue")]
    )
    assert df["uid"].is_unique().all()
    assert set(df["sourceDataset"]) == {"boamp", "joue"}
    joue = set(df.filter(pl.col("sourceDataset") == "joue")["idAvisInitial"])
    assert {"677195-2026", "599757-2026", "631605-2026"} & joue == {
        "677195-2026",
        "599757-2026",
    }
    assert df.filter(pl.col("idAvisInitial") == "599757-2026")[
        "statut"
    ].unique().to_list() == ["annule"]


def test_scrap_open_publie_deux_fichiers(flow_env, deux_sources, publication):
    flow_module.scrap_open(run_date=RUN_DATE)
    assert {p[:2] for p in publication.publies} == {
        ("ds-b", _rid("boamp")),
        ("ds-b", _rid("joue")),
    }
    joue = pl.read_parquet(flow_env / "joue.parquet")
    assert "631605-2026" not in set(joue["idAvisInitial"])
    assert joue.height > 0
    assert dict(joue.schema) == published_schema()
    assert list(joue.columns) == list(published_schema())


def test_scrap_open_lance_deao_processing(flow_env, traitement):
    flow_module.scrap_open(run_date=RUN_DATE)
    assert traitement == [RUN_DATE]


def test_scrap_open_sous_minimum(
    flow_env, deux_sources, publication, traitement, monkeypatch
):
    registre = tuple(
        dataclasses.replace(s, min_rows=10**6) if s.code == "joue" else s
        for s in config.DEAO_REGISTRE
    )
    monkeypatch.setattr(config, "DEAO_REGISTRE", registre)
    with pytest.raises(RuntimeError, match="joue"):
        flow_module.scrap_open(run_date=RUN_DATE)
    assert [p[:2] for p in publication.publies] == [("ds-b", _rid("boamp"))]
    assert traitement == [RUN_DATE]


def test_scrap_open_jour_ted_en_erreur_lance_traitement(
    flow_env,
    deux_sources,
    no_sleep,
    traitement,  # noqa: F811
):
    deux_sources.recherche_en_erreur = {date(2026, 10, 14)}  # 500 persistant
    with pytest.raises(RuntimeError, match="joue"):
        flow_module.scrap_open(run_date=RUN_DATE)
    assert traitement == [RUN_DATE]


def test_scrap_open_sans_publication_si_solo(flow_env, publication, monkeypatch):
    monkeypatch.setattr("src.config.SOLO_DEAO_SCRAPS", ["boamp"])
    flow_module.scrap_open(run_date=RUN_DATE)
    assert publication.publies == []
    assert (flow_env / "boamp.parquet").exists()


@pytest.fixture
def traitement_reel(flow_env, publication, monkeypatch, tmp_path):
    """deao_processing réel, lisant ce que la collecte a publié sur le faux dataset b."""
    monkeypatch.setattr(flow_module, "deao_processing", deao_processing_reel)
    monkeypatch.setattr(config, "DEAO_DIST_DIR", tmp_path / "dist_traitement")
    monkeypatch.setattr(config, "DEAO_DATA_DIR", tmp_path / "data_traitement")
    monkeypatch.setattr(config, "DEAO_OPEN_DATASET_ID", "ds-a")
    monkeypatch.setattr(config, "DEAO_PARQUET_RESOURCE_ID", "res-a-parquet")
    monkeypatch.setattr(config, "DEAO_CSV_RESOURCE_ID", "res-a-csv")
    monkeypatch.setattr(config, "DEAO_SCHEMA_RESOURCE_ID", "res-a-schema")
    monkeypatch.setattr(config, "DEAO_CLOSED_PARQUET_RESOURCE_ID", "res-b-deao")
    monkeypatch.setattr(config, "DEAO_MIN_ROWS_TO_PUBLISH", 1)
    return publication


def _agregat(publication) -> pl.DataFrame:
    import io

    return pl.read_parquet(io.BytesIO(publication.fichiers["res-b-deao"]))


def test_bout_en_bout(flow_env, deux_sources, traitement_reel):
    flow_module.scrap_open(run_date=RUN_DATE)
    df = _agregat(traitement_reel)
    assert set(df["sourceDataset"]) == {"boamp", "joue"}
    assert df["uid"].is_unique().all()


def test_scrap_open_sources_boamp_seul(
    flow_env, deux_sources, traitement_reel, monkeypatch
):
    flow_module.scrap_open(run_date=RUN_DATE)  # publie boamp et joue
    traitement_reel.publies.clear()
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ("boamp",))
    flow_module.scrap_open(run_date=RUN_DATE)
    sources = {p[1] for p in traitement_reel.publies}
    assert _rid("boamp") in sources and _rid("joue") not in sources
    # deao_processing lit encore joue (servi par le faux dataset b)
    assert set(_agregat(traitement_reel)["sourceDataset"]) == {"boamp", "joue"}


def test_flow_jour_joue_en_erreur_fait_echouer(flow_env, deux_sources, no_sleep):  # noqa: F811
    deux_sources.recherche_en_erreur = {date(2026, 10, 14)}  # 500 persistant
    with pytest.raises(RuntimeError, match="joue"):
        flow_module.scrap_open(run_date=RUN_DATE)

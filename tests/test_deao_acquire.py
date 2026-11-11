# ruff: noqa: F811
from datetime import date
from pathlib import Path

import httpx
import polars as pl
import pytest

from src.tasks.deao.sources.boamp import acquire, fetch
from src.tasks.deao.sources.boamp.acquire import (
    Action,
    acquire_boamp,
    local_days,
    partition_path,
    plan_days,
    process_day,
)
from src.tasks.deao.sources.boamp.archive import (
    list_archived_days,
    read_day_archive,
    write_day_archive,
)
from tests.deao_fakes import fake_s3  # noqa: F401

DATA = Path(__file__).parent / "data" / "deao"
XML_FILES = sorted(DATA.glob("*.xml"))


def _index(*names):
    return "".join(f'<a href="{n}">{n}</a>' for n in names)


@pytest.fixture
def partitions(monkeypatch, tmp_path):
    monkeypatch.setattr(acquire, "DEAO_PARTITIONS_DIR", tmp_path)
    return tmp_path


@pytest.fixture
def web(monkeypatch):
    """Faux réseau : dict url -> (statut, texte/bytes) ; trace des URLs appelées."""
    pages: dict[str, tuple[int, str | bytes]] = {}
    calls: list[str] = []

    def http_get(url):
        calls.append(url)
        status, body = pages.get(url, (404, ""))
        if isinstance(body, bytes):
            return httpx.Response(status, content=body)
        return httpx.Response(status, text=body)

    monkeypatch.setattr(fetch, "http_get", http_get)
    web_ = type("Web", (), {})()
    web_.pages, web_.calls = pages, calls
    return web_


def _url(d, historique=False):
    return fetch.day_index_urls(d)[1 if historique else 0]


def test_plan_days():
    end = date(2026, 10, 15)
    plan = dict(
        plan_days(
            date(2026, 10, 10),
            end,
            3,
            local_days={date(2026, 10, 10), date(2026, 10, 14)},
            archived_days={date(2026, 10, 11)},
        )
    )
    assert date(2026, 10, 10) not in plan
    assert plan[date(2026, 10, 11)] == Action.FROM_ARCHIVE
    assert plan[date(2026, 10, 12)] == Action.CRAWL
    assert (
        plan[date(2026, 10, 13)]
        == plan[date(2026, 10, 14)]
        == plan[date(2026, 10, 15)]
        == Action.RECRAWL
    )


def test_list_day_files_repli_flux_historique(web):
    d = date(2025, 3, 12)
    web.pages[_url(d, True)] = (200, _index("26-1.xml", "26-1.xml", "25-2.xml"))
    url, names = fetch.list_day_files(d)
    assert url.endswith("/FluxHistorique/Boamp_v240/2025/03/12/")
    assert names == ["25-2.xml", "26-1.xml"]


def test_list_day_files_absent(web):
    assert fetch.list_day_files(date(2026, 1, 1)) == (None, [])


def test_list_day_files_statut_inattendu(web):
    d = date(2026, 1, 1)
    web.pages[_url(d)] = (500, "")
    with pytest.raises(httpx.HTTPStatusError):
        fetch.list_day_files(d)


def test_download_xml_erreur(web):
    web.pages["http://x/a.xml"] = (500, b"")
    with pytest.raises(httpx.HTTPStatusError):
        fetch.download_xml("http://x/", "a.xml")


def test_local_days_ignore_le_reste(partitions):
    p = partition_path(date(2026, 10, 14))
    p.parent.mkdir(parents=True)
    p.touch()
    p.with_suffix(".parquet.tmp").touch()
    (partitions / "boamp" / "notes.parquet").touch()
    assert local_days() == {date(2026, 10, 14)}


def test_process_day_jour_sans_dossier_ecrit_marqueur(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 1, 1)
    process_day(d, Action.CRAWL, archived=False)
    assert pl.read_parquet(partition_path(d)).height == 0
    assert list_archived_days() == {d}
    assert read_day_archive(d) == {}


def test_process_day_crawl_ecrit_archive_et_partition(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 10, 14)
    xml = XML_FILES[0].read_bytes()
    web.pages[_url(d)] = (200, _index("26-1.xml"))
    web.pages[_url(d) + "26-1.xml"] = (200, xml)
    process_day(d, Action.CRAWL, archived=False)
    assert read_day_archive(d) == {"26-1.xml": xml}
    assert partition_path(d).exists()


def test_process_day_recrawl_differentiel(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 10, 14)
    write_day_archive(d, {"26-99870.xml": XML_FILES[0].read_bytes()})
    web.pages[_url(d)] = (200, _index("26-99870.xml", "26-99891.xml"))
    web.pages[_url(d) + "26-99891.xml"] = (200, XML_FILES[1].read_bytes())
    process_day(d, Action.RECRAWL, archived=True)
    assert [c for c in web.calls if c.endswith(".xml")] == [_url(d) + "26-99891.xml"]
    assert set(read_day_archive(d)) == {"26-99870.xml", "26-99891.xml"}
    assert partition_path(d).exists()


def test_process_day_recrawl_sans_nouveau_ne_reecrit_pas_l_archive(
    partitions,
    web,
    fake_s3,  # noqa: F811
):
    d = date(2026, 10, 14)
    write_day_archive(d, {"26-1.xml": XML_FILES[0].read_bytes()})
    before = dict(fake_s3.objects)
    web.pages[_url(d)] = (200, _index("26-1.xml"))
    process_day(d, Action.RECRAWL, archived=True)
    assert fake_s3.objects == before
    assert partition_path(d).exists()


def test_process_day_from_archive_sans_reseau(partitions, monkeypatch, fake_s3):  # noqa: F811
    def boom(url):
        raise AssertionError("réseau interdit")

    monkeypatch.setattr(fetch, "http_get", boom)
    d = date(2026, 10, 14)
    write_day_archive(d, {"26-1.xml": XML_FILES[0].read_bytes()})
    process_day(d, Action.FROM_ARCHIVE, archived=True)
    assert partition_path(d).exists()


def test_process_day_echec_telechargement_n_ecrit_rien(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 10, 14)
    web.pages[_url(d)] = (200, _index("26-1.xml", "26-2.xml"))
    web.pages[_url(d) + "26-1.xml"] = (200, XML_FILES[0].read_bytes())
    web.pages[_url(d) + "26-2.xml"] = (500, b"")
    with pytest.raises(Exception):
        process_day(d, Action.CRAWL, archived=False)
    assert not partition_path(d).exists()
    assert not list(partitions.rglob("*"))
    assert d not in list_archived_days()


def test_process_day_echec_listing_n_ecrit_rien(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 10, 14)
    web.pages[_url(d)] = (503, "")
    with pytest.raises(Exception):
        process_day(d, Action.CRAWL, archived=False)
    assert not list(partitions.rglob("*"))
    assert list_archived_days() == set()


def test_acquire_rebuild_supprime_les_partitions(partitions, web, fake_s3, monkeypatch):  # noqa: F811
    monkeypatch.setattr(acquire, "BOAMP_RECRAWL_DAYS", 0)
    d = date(2026, 1, 10)
    write_day_archive(d, {"26-1.xml": XML_FILES[0].read_bytes()})
    old = partition_path(d)
    old.parent.mkdir(parents=True)
    pl.DataFrame({"stale": [1]}).write_parquet(old)
    c = acquire_boamp(d, d, rebuild=True)
    assert "stale" not in pl.read_parquet(old).columns
    assert c["jour_en_erreur"] == 0
    assert web.calls == []


def test_acquire_compte_les_jours_en_erreur(partitions, web, fake_s3, monkeypatch):  # noqa: F811
    monkeypatch.setattr(acquire, "BOAMP_RECRAWL_DAYS", 0)
    web.pages[_url(date(2026, 1, 11))] = (500, "")
    c = acquire_boamp(date(2026, 1, 10), date(2026, 1, 11), rebuild=False)
    assert c["jour_en_erreur"] == 1
    assert local_days() == {date(2026, 1, 10)}


def test_recrawl_echec_second_telechargement_laisse_tout_intact(
    partitions, web, fake_s3
):  # noqa: F811
    d = date(2026, 10, 14)
    write_day_archive(d, {"26-1.xml": XML_FILES[0].read_bytes()})
    process_day(d, Action.FROM_ARCHIVE, archived=True)
    before_objects = dict(fake_s3.objects)
    before_part = partition_path(d).read_bytes()
    web.pages[_url(d)] = (200, _index("26-1.xml", "26-2.xml", "26-3.xml"))
    web.pages[_url(d) + "26-2.xml"] = (200, XML_FILES[1].read_bytes())
    web.pages[_url(d) + "26-3.xml"] = (500, b"")
    with pytest.raises(Exception):
        process_day(d, Action.RECRAWL, archived=True)
    assert fake_s3.objects == before_objects
    assert partition_path(d).read_bytes() == before_part


def test_recrawl_non_archive_ecrit_archive_complete(partitions, web, fake_s3):  # noqa: F811
    d = date(2026, 10, 14)
    web.pages[_url(d)] = (200, _index("26-1.xml", "26-2.xml"))
    web.pages[_url(d) + "26-1.xml"] = (200, XML_FILES[0].read_bytes())
    web.pages[_url(d) + "26-2.xml"] = (200, XML_FILES[1].read_bytes())
    process_day(d, Action.RECRAWL, archived=False)
    assert set(read_day_archive(d)) == {"26-1.xml", "26-2.xml"}


def test_rebuild_jour_vide_archive_sans_reseau(partitions, monkeypatch, fake_s3):  # noqa: F811
    monkeypatch.setattr(acquire, "BOAMP_RECRAWL_DAYS", 0)
    d = date(2026, 1, 10)
    write_day_archive(d, {})

    def boom(url):
        raise AssertionError("réseau interdit")

    monkeypatch.setattr(fetch, "http_get", boom)
    c = acquire_boamp(d, d, rebuild=True)
    assert c["jour_en_erreur"] == 0
    assert pl.read_parquet(partition_path(d)).height == 0


def test_process_day_echec_parsing_archive_ecrite_sans_partition(
    partitions,
    web,
    fake_s3,  # noqa: F811
    monkeypatch,
):
    d = date(2026, 10, 14)
    xml = XML_FILES[0].read_bytes()
    web.pages[_url(d)] = (200, _index("26-1.xml"))
    web.pages[_url(d) + "26-1.xml"] = (200, xml)

    def boom(files):
        raise RuntimeError("parsing")

    monkeypatch.setattr(acquire, "parse_day", boom)
    with pytest.raises(RuntimeError):
        process_day(d, Action.CRAWL, archived=False)
    assert read_day_archive(d) == {"26-1.xml": xml}
    assert not partition_path(d).exists()
    assert not list(partitions.rglob("*.parquet*"))


def test_process_day_compte_les_jours_vides(partitions, web, fake_s3):  # noqa: F811
    vide = date(2026, 1, 1)
    c = process_day(vide, Action.CRAWL, archived=False)
    assert c["jour_vide"] == 1
    d = date(2026, 10, 14)
    web.pages[_url(d)] = (200, _index("26-1.xml"))
    web.pages[_url(d) + "26-1.xml"] = (200, XML_FILES[0].read_bytes())
    assert process_day(d, Action.CRAWL, archived=False)["jour_vide"] == 0

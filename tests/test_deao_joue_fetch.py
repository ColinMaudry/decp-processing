from datetime import date

import httpx
import pytest

from src.tasks.deao.sources.joue.fetch import (
    download_xml,
    list_day_files,
    numero_publication,
    query_jour,
)
from tests.deao_fakes import XML_TED_TEST, fake_ted, no_sleep  # noqa: F401


def test_query_jour():
    q = query_jour(date(2026, 10, 14))
    assert "buyer-country IN (FRA ATF PYF NCL WLF SPM BLM MAF)" in q
    assert "place-of-performance IN (FRA ATF PYF NCL WLF SPM BLM MAF)" in q
    assert q.endswith("publication-date=20261014")


def test_list_day_files_pagination(fake_ted):  # noqa: F811
    fake_ted.avis_par_jour[date(2026, 10, 14)] = {
        f"{n}-2026": XML_TED_TEST for n in range(700000, 700253)
    }
    base, noms = list_day_files(date(2026, 10, 14))
    assert base is None and len(noms) == 253 and noms == sorted(set(noms))
    assert sum(a.startswith("POST") for a in fake_ted.appels) == 2


def test_list_day_files_jour_vide(fake_ted):  # noqa: F811
    assert list_day_files(date(2026, 10, 18)) == (None, [])


def test_429_puis_succes(fake_ted, no_sleep):  # noqa: F811
    fake_ted.reponses_xml["706679-2026"] = [429, 200]
    assert download_xml(None, "706679-2026.xml").startswith(b"<?xml")
    assert len(fake_ted.appels) == 2


def test_404_sans_nouvel_essai(fake_ted, no_sleep):  # noqa: F811
    with pytest.raises(httpx.HTTPStatusError):
        download_xml(None, "inconnu.xml")
    assert len(fake_ted.appels) == 1


def test_recherche_en_erreur_epuise_les_essais(fake_ted, no_sleep):  # noqa: F811
    fake_ted.recherche_en_erreur.add(date(2026, 10, 15))
    with pytest.raises(httpx.HTTPStatusError):
        list_day_files(date(2026, 10, 15))
    assert len(fake_ted.appels) == 5


def test_numero_publication(fake_ted):  # noqa: F811
    assert numero_publication("c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b") == "677195-2026"
    assert numero_publication("00000000-0000-0000-0000-000000000000") is None

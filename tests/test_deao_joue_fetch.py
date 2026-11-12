from datetime import date

import httpx
import pytest

from src.tasks.scrap.deao.boamp_joue.joue import fetch
from src.tasks.scrap.deao.boamp_joue.joue.fetch import (
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
    assert len(fake_ted.appels) == 6


def test_numero_publication(fake_ted):  # noqa: F811
    assert numero_publication("c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b") == "677195-2026"
    assert numero_publication("00000000-0000-0000-0000-000000000000") is None


def test_numero_publication_sans_zeros_de_tete(monkeypatch):
    monkeypatch.setattr(fetch, "_rechercher", lambda q: ["00677195-2026"])
    assert numero_publication("x") == "677195-2026"


def test_query_jour_sans_code_pays(monkeypatch):
    monkeypatch.setattr(fetch, "JOUE_COUNTRY_CODES", ())
    with pytest.raises(ValueError, match="JOUE_COUNTRY_CODES"):
        query_jour(date(2026, 10, 14))


# --- Limitation du débit vers TED (issue #203) ---------------------------------


class _Horloge:
    """Horloge et sommeil simulés : `dormir` avance le temps sans attendre."""

    def __init__(self):
        self.t = 100.0
        self.sommeils: list[float] = []

    def __call__(self) -> float:
        return self.t

    def dormir(self, s: float) -> None:
        self.sommeils.append(s)
        self.t += s


def test_limiteur_espace_les_requetes():
    h = _Horloge()
    lim = fetch.Limiteur(0.6, horloge=h, dormir=h.dormir)
    instants = []
    for _ in range(4):
        lim.attendre()
        instants.append(h.t)
    assert [round(b - a, 6) for a, b in zip(instants, instants[1:])] == [0.6, 0.6, 0.6]


def test_limiteur_sans_attente_si_intervalle_deja_ecoule():
    h = _Horloge()
    lim = fetch.Limiteur(0.6, horloge=h, dormir=h.dormir)
    lim.attendre()
    h.t += 5
    lim.attendre()
    assert h.sommeils == []


def test_limiteur_partage_entre_fils():
    import threading

    h = _Horloge()
    verrou = threading.Lock()

    def dormir(s):
        with verrou:
            h.t += s

    lim = fetch.Limiteur(0.6, horloge=h, dormir=dormir)
    instants = []

    def tache():
        lim.attendre()
        instants.append(h.t)

    fils = [threading.Thread(target=tache) for _ in range(8)]
    for f in fils:
        f.start()
    for f in fils:
        f.join()
    instants.sort()
    assert all(b - a >= 0.6 - 1e-9 for a, b in zip(instants, instants[1:]))


def test_get_raw_passe_par_le_limiteur(monkeypatch):
    appels = []
    monkeypatch.setattr(
        fetch,
        "_LIMITEUR",
        type("L", (), {"attendre": lambda self: appels.append("attente")})(),
    )
    monkeypatch.setattr(
        fetch.HTTP_CLIENT,
        "get",
        lambda *a, **k: appels.append("get") or httpx.Response(200),
    )
    monkeypatch.setattr(
        fetch.HTTP_CLIENT,
        "post",
        lambda *a, **k: appels.append("post") or httpx.Response(200),
    )
    fetch._get_raw("https://ted.europa.eu/x")
    fetch._post_raw("https://api.ted.europa.eu/v3/notices/search", {})
    assert appels == ["attente", "get", "attente", "post"]


def test_attente_avant_nouvel_essai_apres_429():
    """Après une 429, on attend au moins JOUE_ATTENTE_429_S avant de réessayer."""
    from tenacity import RetryCallState

    etat = RetryCallState(
        retry_object=fetch.http_get.retry, fn=None, args=(), kwargs={}
    )
    etat.attempt_number = 1
    assert fetch.http_get.retry.wait(etat) >= fetch.JOUE_ATTENTE_429_S

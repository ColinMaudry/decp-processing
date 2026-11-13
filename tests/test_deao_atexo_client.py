import re
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
from lxml import html as lxml_html

from src.tasks.scrap.deao.atexo.client import (
    ClientAtexo,
    DceTelecharge,
    RechercheIncomplete,
)
from src.tasks.scrap.deao.atexo.parse import DetailConsultation
from src.tasks.scrap.deao.limiteur import Limiteur

FIXTURES = Path(__file__).parent / "data" / "atexo"
BASE = "https://marches.megalis.bretagne.bzh"
R = "ctl0$CONTENU_PAGE$resultSearch$"


def _fixture(nom: str) -> str:
    return (FIXTURES / nom).read_text()


def _page(nom: str, total: int, n_blocs: int) -> str:
    """Page de résultats réelle réduite à `n_blocs` blocs, compteur forcé à `total`."""
    arbre = lxml_html.fromstring(_fixture(nom))
    for bloc in arbre.xpath('//div[contains(@class, "item_consultation")]')[n_blocs:]:
        bloc.getparent().remove(bloc)
    texte = lxml_html.tostring(arbre, encoding="unicode")
    return re.sub(
        r'(id="ctl0_CONTENU_PAGE_resultSearch_nombreElement">)\s*\d+',
        rf"\g<1>{total}",
        texte,
    )


class FauxSite:
    """Faux site Atexo : enregistre les requêtes, sert des réponses par cible PRADO."""

    def __init__(self, pages_recherche: dict[str, str] | None = None):
        self.requetes: list[httpx.Request] = []
        self.pages_recherche = pages_recherche or {}
        self.detail: list[int] = []
        self.fichier: bytes | None = None

    def formulaire(self, requete: httpx.Request) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(requete.content.decode()).items()}

    def __call__(self, requete: httpx.Request) -> httpx.Response:
        self.requetes.append(requete)
        page = requete.url.params.get("page", "")
        if requete.url.path.startswith("/entreprise/consultation/"):
            code = self.detail.pop(0) if self.detail else 200
            texte = _fixture("megalis_detail_242752.html") if code == 200 else ""
            return httpx.Response(code, text=texte)
        if page == "Entreprise.PopUpDetailLots":
            return httpx.Response(200, text=_fixture("megalis_lots_242752.html"))
        if "searchAnnCons" in str(requete.url):
            if requete.method == "GET":
                return httpx.Response(200, text=_fixture("megalis_recherche.html"))
            champs = self.formulaire(requete)
            cible = champs["PRADO_POSTBACK_TARGET"]
            if cible == f"{R}DefaultButtonTop":
                cible = f"page{champs[f'{R}numPageTop']}"
            if cible not in self.pages_recherche:
                return httpx.Response(500)
            return httpx.Response(200, text=self.pages_recherche[cible])
        if page == "Entreprise.EntrepriseDemandeTelechargementDce":
            if requete.method == "GET":
                return httpx.Response(200, text=_fixture("megalis_dce_formulaire.html"))
            cible = self.formulaire(requete)["PRADO_POSTBACK_TARGET"]
            if cible == "ctl0$CONTENU_PAGE$validateButton":
                return httpx.Response(200, text=_fixture("megalis_dce_validation.html"))
            if cible == "ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload":
                taille, morceau = 5 * 1024 * 1024, 64 * 1024

                def morceaux():
                    for _ in range(taille // morceau):
                        yield b"x" * morceau

                return httpx.Response(
                    200,
                    headers={
                        "Content-Type": "application/zip",
                        "Content-Disposition": 'attachment; filename="1-DCE.zip";',
                    },
                    content=morceaux(),
                )
        return httpx.Response(404)


class LimiteurCompteur(Limiteur):
    def __init__(self):
        super().__init__(0)
        self.appels = 0

    def attendre(self) -> None:
        self.appels += 1
        super().attendre()


def _client(site: FauxSite, limiteur: Limiteur | None = None) -> ClientAtexo:
    http = httpx.Client(transport=httpx.MockTransport(site))
    return ClientAtexo(BASE, http=http, limiteur=limiteur or Limiteur(0))


@pytest.fixture
def no_sleep(monkeypatch):
    for fn in (ClientAtexo._requete, ClientAtexo._telecharger_flux):
        monkeypatch.setattr(fn.retry, "sleep", lambda _: None)


def _pages_25() -> dict[str, str]:
    return {
        "ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche": _page(
            "megalis_resultats_20.html", 25, 10
        ),
        f"{R}listePageSizeTop": _page("megalis_resultats_20.html", 25, 20),
        "page2": _page("megalis_resultats.html", 25, 5),
    }


def test_recherche_toutes_pages():
    site = FauxSite(_pages_25())
    consultations = _client(site).rechercher()

    assert len(consultations) == 25
    assert len({c.ref_cons for c in consultations}) == 25

    posts = [site.formulaire(r) for r in site.requetes if r.method == "POST"]
    assert len(posts) == 3
    recherche = posts[0]
    assert recherche["ctl0$CONTENU_PAGE$AdvancedSearch$dateMiseEnLigneStart"] == (
        "01/01/2000"
    )
    fin = recherche["ctl0$CONTENU_PAGE$AdvancedSearch$dateMiseEnLigneEnd"]
    assert re.fullmatch(r"\d{2}/\d{2}/\d{4}", fin)
    assert posts[1][f"{R}listePageSizeTop"] == "20"
    assert posts[2][f"{R}numPageTop"] == "2"
    # Champs lus dans la page précédente (état PRADO)
    assert all("PRADO_PAGESTATE" in p for p in posts)


def test_recherche_incomplete_leve():
    site = FauxSite(
        {
            "ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche": _page(
                "megalis_resultats_20.html", 45, 10
            ),
            f"{R}listePageSizeTop": _page("megalis_resultats_20.html", 45, 20),
            "page2": _page("megalis_resultats_20.html", 45, 0),
            "page3": _page("megalis_resultats_20.html", 45, 0),
        }
    )
    with pytest.raises(RechercheIncomplete) as exc:
        _client(site).rechercher()
    # Les consultations lues restent disponibles pour le scraper.
    assert len(exc.value.consultations) == 20
    assert exc.value.annoncees == 45


def test_recherche_page_en_erreur_leve(no_sleep):
    site = FauxSite({k: v for k, v in _pages_25().items() if k != "page2"})
    with pytest.raises(RechercheIncomplete) as exc:
        _client(site).rechercher()
    assert len(exc.value.consultations) == 20
    assert exc.value.annoncees == 25


def test_recherche_sans_resultat():
    site = FauxSite(
        {
            "ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche": _fixture(
                "megalis_aucun_resultat.html"
            )
        }
    )
    assert _client(site).rechercher() == []
    assert sum(r.method == "POST" for r in site.requetes) == 1


def test_detail_et_lots():
    site = FauxSite()
    client = _client(site)
    detail = client.detail("242752", "a4h")
    lots = client.lots("242752", "a4h")

    assert isinstance(detail, DetailConsultation)
    assert lots
    assert str(site.requetes[0].url) == (
        f"{BASE}/entreprise/consultation/242752?orgAcronyme=a4h"
    )
    assert str(site.requetes[1].url) == (
        f"{BASE}/index.php?page=Entreprise.PopUpDetailLots"
        "&orgAccronyme=a4h&id=242752&lang="
    )


def test_dce_en_streaming(tmp_path):
    site = FauxSite()
    dce = _client(site).telecharger_dce("242039", "a1w", tmp_path)

    assert isinstance(dce, DceTelecharge)
    assert dce.nom_fichier == "1-DCE.zip"
    assert dce.content_type == "application/zip"
    assert dce.id_dce is None
    assert dce.chemin.parent == tmp_path
    assert dce.chemin.stat().st_size == 5 * 1024 * 1024

    validation = site.formulaire(site.requetes[1])
    assert validation["ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup"] == (
        "ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme"
    )


def test_429_puis_succes(no_sleep):
    site = FauxSite()
    site.detail = [429, 200]
    assert isinstance(_client(site).detail("242752", "a4h"), DetailConsultation)
    assert len(site.requetes) == 2


def test_404_sans_nouvel_essai(no_sleep):
    site = FauxSite()
    site.detail = [404]
    with pytest.raises(httpx.HTTPStatusError):
        _client(site).detail("242752", "a4h")
    assert len(site.requetes) == 1


def test_limiteur_appele_a_chaque_requete(tmp_path):
    site = FauxSite(_pages_25())
    limiteur = LimiteurCompteur()
    client = _client(site, limiteur)
    client.rechercher()
    client.detail("242752", "a4h")
    client.lots("242752", "a4h")
    client.telecharger_dce("242039", "a1w", tmp_path)

    assert limiteur.appels == len(site.requetes) == 9


def test_recherche_bloc_ampute_incomplete():
    page = _page("megalis_resultats_20.html", 20, 20)
    arbre = lxml_html.fromstring(page)
    bloc = arbre.xpath('//div[contains(@class, "item_consultation")]')[5]
    for ligne in bloc.xpath('.//div[contains(@class, "objet-line")]'):
        ligne.getparent().remove(ligne)
    site = FauxSite(
        {
            "ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche": _page(
                "megalis_resultats_20.html", 20, 10
            ),
            f"{R}listePageSizeTop": lxml_html.tostring(arbre, encoding="unicode"),
        }
    )
    with pytest.raises(RechercheIncomplete) as exc:
        _client(site).rechercher()
    assert exc.value.annoncees == 20

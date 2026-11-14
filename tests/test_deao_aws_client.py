from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from src.tasks.scrap.deao.aws.client import (
    PROCEDURES,
    ClientAws,
    ResultatRecherche,
    url_rc_plateforme,
)
from src.tasks.scrap.deao.aws.parse import numeros_avis
from src.tasks.scrap.deao.limiteur import Limiteur

DOSSIER = Path(__file__).parent / "data" / "aws"
BASE = "https://www.marches-publics.info"
PANNEAUX = {"t": "1", "s": "2", "f": "3"}


def lire(nom: str) -> str:
    return (DOSSIER / nom).read_text(encoding="utf-8")


def bloc(numero: int, avec_idm: bool = True) -> str:
    lien_idm = (
        f'<a href="/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={numero + 5000}">RC</a>'
        if avec_idm
        else ""
    )
    return f"""<div class="container-fluid" id="entity">
    Publié le 10/11/26 Date limite : le 02/12/26 à 12h00
    <h2 class="h2-avis">ACHETEUR {numero} (13001)</h2>
    <div id='titre_box'>Objet {numero}</div>
    <a href="/Annonces/MPI-pub-{numero}.htm">avis</a>{lien_idm}</div>"""


def liste(badges: dict[str, int] | None = None, **blocs: list[str]) -> str:
    """Page de résultats synthétique : `s=[blocs]` met les blocs dans l'onglet services."""
    badges = badges or {}
    pied = "".join(
        f'<div class="tab-pane" id="{PANNEAUX[o]}">{"".join(b)}</div>'
        for o, b in blocs.items()
    )
    entete = "".join(
        f'<span class="badge" id="badge_{nom}">{badges.get(o, len(blocs.get(o, [])))}</span>'
        for o, nom in (("t", "travaux"), ("s", "services"), ("f", "fournitures"))
    )
    return f"<html><body>{entete}{pied}</body></html>"


FORMULAIRE = (
    '<html><body><select name="IDR"><option value="X">Toutes</option>'
    '<option value="13">13</option></select></body></html>'
)


class FauxSite:
    """Faux site : POST selon (IDR, IDN, IDP), pagination GET selon (onglet, n°)."""

    def __init__(self, formulaire: str = FORMULAIRE):
        self.formulaire = formulaire
        self.postes: dict[tuple[str, str, str], list[str]] = {}
        self.pages: dict[tuple[str, int], str] = {}
        self.requetes: list[tuple[str, str]] = []
        self.corps: list[dict[str, str]] = []

    def poster(self, idr: str, idn: str = "X", idp: str = "X", *html: str):
        self.postes[(idr, idn, idp)] = list(html)

    def __call__(self, requete: httpx.Request) -> httpx.Response:
        url = str(requete.url)
        self.requetes.append((requete.method, url))
        if requete.method == "GET" and requete.url.path == "/Annonces/rechercher":
            return httpx.Response(200, text=self.formulaire)
        if requete.method == "POST":
            champs = {k: v[0] for k, v in parse_qs(requete.content.decode()).items()}
            self.corps.append(champs)
            cle = (champs["IDR"], champs["IDN"], champs["IDP"])
            reponses = self.postes.get(cle)
            if not reponses:
                return httpx.Response(200, text=liste())
            html = reponses.pop(0) if len(reponses) > 1 else reponses[0]
            return httpx.Response(200, text=html)
        if requete.url.path == "/Annonces/lister":
            (cle, numero), *_ = [
                (k, int(v[0])) for k, v in parse_qs(requete.url.query.decode()).items()
            ]
            page = self.pages.get((cle.removeprefix("pager_"), numero))
            if page is None:
                return httpx.Response(404, text="introuvable")
            return httpx.Response(200, text=page)
        if requete.url.path.startswith("/Annonces/MPI-pub-"):
            return httpx.Response(200, text="<html>avis</html>")
        return httpx.Response(404)

    def client(self) -> ClientAws:
        return ClientAws(
            BASE,
            http=httpx.Client(
                transport=httpx.MockTransport(self), follow_redirects=True
            ),
            limiteur=Limiteur(0),
        )


def test_decoupage_recursif():
    site = FauxSite(lire("rechercher.html"))
    site.poster("X", "X", "X", lire("liste_trop_large.html"))
    site.poster("13", "X", "X", lire("liste_trop_large.html"))
    page1 = lire("liste_13_S_p1.html")
    site.poster("13", "S", "X", page1)
    numeros_p1 = numeros_avis(page1, "s")
    assert len(numeros_p1) == 10
    # pages 2 à 7 : 57 autres avis, 10 par page
    suivants = iter(range(9000, 9057))
    for n in range(2, 8):
        site.pages[("s", n)] = liste(
            s=[bloc(next(suivants)) for _ in range(10 if n < 7 else 7)]
        )
    resultat = site.client().rechercher()

    natures = [c["IDN"] for c in site.corps if c["IDR"] == "13"]
    assert natures == ["X", "T", "S", "F"]
    assert all(c["IDP"] == "X" for c in site.corps)
    assert len(resultat.avis) == 67
    assert resultat.total_avant == resultat.total_apres == 4093
    assert resultat.segments_en_erreur == []


def test_pagination_onglet():
    site = FauxSite()
    site.poster("X", "X", "X", liste(s=[bloc(n) for n in range(12)]))
    site.poster("13", "X", "X", liste({"s": 12}, s=[bloc(n) for n in range(10)]))
    site.pages[("s", 2)] = liste(s=[bloc(10), bloc(11)])
    resultat = site.client().rechercher()
    assert ("GET", f"{BASE}/Annonces/lister?pager_s=2") in site.requetes
    assert len(resultat.avis) == 12
    assert resultat.segments_en_erreur == []
    assert resultat.complete


@pytest.mark.parametrize(
    "page2",
    [None, liste({"s": 12}), lire("rechercher.html")],
    ids=["404", "vide", "formulaire"],
)
def test_onglet_incomplet_segment_en_erreur(page2):
    site = FauxSite()
    site.poster("X", "X", "X", liste({"s": 12}, s=[bloc(n) for n in range(10)]))
    site.poster("13", "X", "X", liste({"s": 12}, s=[bloc(n) for n in range(10)]))
    site.poster("13", "S", "X", liste({"s": 12}, s=[bloc(n) for n in range(10)]))
    for code in PROCEDURES:
        site.poster("13", "S", code, liste({"s": 12}, s=[bloc(n) for n in range(10)]))
    if page2 is not None:
        site.pages[("s", 2)] = page2
    resultat = site.client().rechercher()
    assert resultat.segments_en_erreur == [f"13/S/{code}" for code in PROCEDURES]
    assert not resultat.complete
    assert len(resultat.avis) == 10
    # nouvel essai avant le découpage : le segment département est posté deux fois
    assert [c["IDN"] for c in site.corps if c["IDR"] == "13"].count("X") == 2


def test_decoupage_par_procedure_rattrape_les_pages_manquantes():
    site = FauxSite()
    site.poster("X", "X", "X", liste({"s": 12}))
    dix = liste({"s": 12}, s=[bloc(n) for n in range(10)])
    site.poster("13", "X", "X", dix)
    site.poster("13", "S", "X", dix)
    site.poster("13", "S", "PA", liste(s=[bloc(10), bloc(11)]))
    resultat = site.client().rechercher()
    assert resultat.segments_en_erreur == []
    assert len(resultat.avis) == 12
    assert resultat.complete


def test_avis_sans_idm_comptes_pour_la_completude():
    site = FauxSite()
    page = liste(s=[bloc(1), bloc(2, avec_idm=False), bloc(3), bloc(4, avec_idm=False)])
    site.poster("X", "X", "X", page)
    site.poster("13", "X", "X", page)
    resultat = site.client().rechercher()
    assert [a.numero_avis for a in resultat.avis] == ["1", "3"]
    assert resultat.sans_idm == 2
    assert resultat.complete


def test_avis_dans_deux_departements():
    site = FauxSite(
        FORMULAIRE.replace(
            '<option value="13">13</option>',
            '<option value="25">25</option><option value="90">90</option>',
        )
    )
    page = liste(s=[bloc(1), bloc(2), bloc(3)])
    for cle in ("X", "25", "90"):
        site.poster(cle, "X", "X", page)
    resultat = site.client().rechercher()
    assert len(resultat.avis) == 3 and resultat.sans_idm == 0
    assert resultat.complete


def test_completude_tolerance():
    def resultat(lus: int, sans_idm: int = 0) -> ResultatRecherche:
        return ResultatRecherche(
            avis=[object()] * (lus - sans_idm),  # type: ignore[list-item]
            total_avant=100,
            total_apres=103,
            sans_idm=sans_idm,
        )

    assert resultat(101).complete
    assert resultat(100).complete and resultat(103).complete
    assert resultat(101, sans_idm=5).complete
    assert not resultat(98).complete
    assert not resultat(104).complete
    assert not ResultatRecherche([], None, 3).complete
    assert not ResultatRecherche([], 0, 0, ["13/S/PA"]).complete


def test_aucune_requete_dce():
    site = FauxSite(lire("rechercher.html"))
    site.poster("X", "X", "X", lire("liste_25.html"))
    site.poster("25", "X", "X", lire("liste_25.html"))
    client = site.client()
    client.rechercher()
    client.page_avis("/Annonces/MPI-pub-2026314252.htm")
    urls = [u for _, u in site.requetes] + [str(c) for c in site.corps]
    assert urls
    assert not [
        u for u in urls if "dematEnt" in u or "avertissement" in u or "type=DCE" in u
    ]


def test_telecharger_rc(tmp_path):
    envoyes = []

    def handler(requete: httpx.Request) -> httpx.Response:
        envoyes.append(str(requete.url))
        return httpx.Response(
            200, content=b"%PDF-1.4 rc", headers={"content-type": "application/pdf"}
        )

    client = ClientAws(
        BASE,
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        limiteur=Limiteur(0),
    )
    chemin = client.telecharger_rc("1902107", tmp_path)
    assert chemin == tmp_path / "1902107.pdf" and chemin.read_bytes() == b"%PDF-1.4 rc"
    assert envoyes == [url_rc_plateforme(BASE, "1902107")]
    assert url_rc_plateforme(BASE, "1902107").endswith("dce.dRC&IDM=1902107")


def test_telecharger_rc_html_leve(tmp_path):
    def handler(requete: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html>captcha</html>",
            headers={"content-type": "text/html; charset=utf-8"},
        )

    client = ClientAws(
        BASE,
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        limiteur=Limiteur(0),
    )
    with pytest.raises(ValueError, match="HTML"):
        client.telecharger_rc("1902107", tmp_path)


def test_page_avis():
    def handler(requete: httpx.Request) -> httpx.Response:
        assert requete.url.path == "/Annonces/MPI-pub-1.htm"
        return httpx.Response(200, text="<html>avis</html>")

    client = ClientAws(
        BASE,
        http=httpx.Client(transport=httpx.MockTransport(handler)),
        limiteur=Limiteur(0),
    )
    assert client.page_avis("/Annonces/MPI-pub-1.htm") == "<html>avis</html>"


def test_post_sans_badge_n_est_pas_zero_resultat():
    site = FauxSite()
    site.poster("X", "X", "X", liste({"s": 12}))
    # le POST du département renvoie le formulaire (aucun badge), deux fois
    site.poster("13", "X", "X", lire("rechercher.html"))
    site.poster("13", "S", "X", lire("rechercher.html"))
    for code in PROCEDURES:
        site.poster("13", "S", code, lire("rechercher.html"))
    resultat = site.client().rechercher()
    assert not resultat.complete
    assert resultat.segments_en_erreur == [f"13/S/{code}" for code in PROCEDURES]


def test_nouveau_post_sans_badge_ne_fait_pas_oublier_le_badge():
    site = FauxSite()
    dix = liste({"s": 12}, s=[bloc(n) for n in range(10)])
    site.poster("X", "X", "X", liste({"s": 12}))
    site.poster("13", "X", "X", dix, lire("rechercher.html"))
    site.poster("13", "S", "X", lire("rechercher.html"))
    for code in PROCEDURES:
        site.poster("13", "S", code, lire("rechercher.html"))
    resultat = site.client().rechercher()
    assert not resultat.complete
    assert resultat.segments_en_erreur


def test_decoupage_sans_couverture_du_badge_note_le_parent():
    site = FauxSite()
    dix = liste({"s": 12}, s=[bloc(n) for n in range(10)])
    site.poster("X", "X", "X", liste({"s": 12}))
    site.poster("13", "X", "X", dix)
    site.poster("13", "S", "X", dix)  # page 2 manquante, procédures toutes vides
    resultat = site.client().rechercher()
    assert resultat.segments_en_erreur == ["13/S"]
    assert not resultat.complete


def test_procedures_sans_resultat_ne_sont_pas_des_erreurs():
    vide = lire("aucun_resultat.html")
    site = FauxSite()
    dix = liste({"s": 12}, s=[bloc(n) for n in range(10)])
    site.poster("X", "X", "X", liste({"s": 12}))
    site.poster("13", "X", "X", dix)
    site.poster("13", "S", "X", dix)  # page 2 manquante : découpage par procédure
    for code in PROCEDURES:
        site.poster("13", "S", code, vide)
    site.poster("13", "S", "PA", liste(s=[bloc(n) for n in range(12)]))
    resultat = site.client().rechercher()
    assert resultat.segments_en_erreur == []
    assert len(resultat.avis) == 12
    assert resultat.complete
    # aucune procédure vide n'est postée deux fois
    postes = [c["IDP"] for c in site.corps if c["IDN"] == "S"]
    assert postes.count("AOE") == 1


def test_nature_sans_resultat_non_reposte():
    vide = lire("aucun_resultat.html")
    site = FauxSite()
    site.poster("X", "X", "X", liste({"t": 3}))
    site.poster("13", "X", "X", lire("liste_trop_large.html"))
    site.poster("13", "T", "X", liste(t=[bloc(1), bloc(2), bloc(3)]))
    site.poster("13", "S", "X", vide)
    site.poster("13", "F", "X", vide)
    resultat = site.client().rechercher()
    assert resultat.segments_en_erreur == []
    assert [c["IDN"] for c in site.corps if c["IDR"] == "13"] == ["X", "T", "S", "F"]
    assert resultat.complete

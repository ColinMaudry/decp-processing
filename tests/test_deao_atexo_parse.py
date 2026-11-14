from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from lxml import html as lxml_html

from src.tasks.scrap.deao.atexo.parse import (
    DetailConsultation,
    champs_formulaire,
    date_fr,
    date_heure_fr,
    nombre_resultats,
    parse_detail,
    parse_lots,
    parse_resultats,
)

FIX = Path(__file__).parent / "data" / "atexo"


def lire(nom: str) -> str:
    return (FIX / nom).read_text(encoding="utf-8")


def test_resultats_megalis():
    html = lire("megalis_resultats.html")
    assert nombre_resultats(html) == 45
    cs = parse_resultats(html)
    assert len(cs) == 10
    c = cs[0]
    assert (c.ref_cons, c.org_cons, c.procedure_code, c.categorie) == (
        "242752",
        "a4h",
        "MAPA",
        "Travaux",
    )
    assert c.reference == "26190042" and c.date_mise_en_ligne == date(2026, 10, 19)
    assert (c.organisme, c.code_postal, c.ville) == (
        "RENNES METROPOLE",
        "35000",
        "Rennes",
    )
    assert c.nombre_lots == 2 and c.lieux_execution == ["35"]
    assert c.date_limite == datetime(
        2026, 11, 13, 16, 45, tzinfo=ZoneInfo("Europe/Paris")
    )
    assert "GESTION DES PROBLÉMATIQUES" in c.objet
    assert c.intitule.startswith("RUE CORENTIN CARRE")


def test_resultats_maximilien():
    html = lire("maximilien_resultats.html")
    cs = parse_resultats(html)
    assert nombre_resultats(html) == 24 and len(cs) == 10
    assert all(c.ref_cons.isdigit() for c in cs)


def test_aucun_resultat():
    html = lire("megalis_aucun_resultat.html")
    assert nombre_resultats(html) == 0 and parse_resultats(html) == []


def test_detail():
    d = parse_detail(lire("megalis_detail_242752.html"))
    assert d == DetailConsultation(
        "45232410", "RENNES METROPOLE", d.service, "72,09 Mo"
    )
    assert "Service Marchés publics" in d.service


def test_lots():
    lots = parse_lots(lire("megalis_lots_242752.html"))
    assert [(lot.numero, lot.intitule, lot.code_cpv) for lot in lots] == [
        ("1", "ASSAINISSEMENT", "45232410"),
        ("2", "VOIRIE", "45233140"),
    ]
    assert lots[0].categorie == "Travaux"
    assert lots[0].description == "ASSAINISSEMENT"


@pytest.mark.parametrize(
    "texte,attendu",
    [
        ("19 Oct. 2026", date(2026, 10, 19)),
        ("15 Juil. 2026", date(2026, 7, 15)),
        ("27 Août 2026", date(2026, 8, 27)),
        ("10 Sept. 2026", date(2026, 9, 10)),
        ("25 Janv. 2027", date(2027, 1, 25)),
        ("31 Déc. 2027", date(2027, 12, 31)),
        ("1 Févr. 2027", date(2027, 2, 1)),
        ("3 Mai 2027", date(2027, 5, 3)),
    ],
)
def test_date_fr(texte, attendu):
    assert date_fr(texte) == attendu


def test_date_fr_mois_inconnu():
    with pytest.raises(ValueError, match="Foo"):
        date_fr("3 Foo. 2027")


def test_date_heure_fr():
    assert date_heure_fr("13 Nov. 2026 16:45") == datetime(
        2026, 11, 13, 16, 45, tzinfo=ZoneInfo("Europe/Paris")
    )


def test_champs_formulaire_recherche():
    champs = champs_formulaire(lire("megalis_resultats.html"))
    assert "PRADO_PAGESTATE" in champs
    assert "ctl0$CONTENU_PAGE$resultSearch$numPageTop" in champs


def test_nombre_resultats_page_inattendue_leve():
    with pytest.raises(ValueError, match="Erreur"):
        nombre_resultats("<html><body>Erreur</body></html>")


@pytest.mark.parametrize(
    "texte,attendu",
    [
        ("5 Juill. 2026", date(2026, 7, 5)),
        ("5 Sep. 2026", date(2026, 9, 5)),
        ("5 Jan. 2026", date(2026, 1, 5)),
        ("5 Fev. 2026", date(2026, 2, 5)),
        ("5 Fév. 2026", date(2026, 2, 5)),  # PLACE
        ("5 Dec. 2026", date(2026, 12, 5)),
    ],
)
def test_date_fr_abreviations_supplementaires(texte, attendu):
    assert date_fr(texte) == attendu


def _sans_objet_line(html: str, n: int) -> str:
    """Page de résultats dont le bloc n° `n` a perdu sa ligne « objet »."""
    arbre = lxml_html.fromstring(html)
    bloc = arbre.xpath('//div[contains(@class, "item_consultation")]')[n]
    for ligne in bloc.xpath('.//div[contains(@class, "objet-line")]'):
        ligne.getparent().remove(ligne)
    return lxml_html.tostring(arbre, encoding="unicode")


def test_bloc_ampute_leve_value_error():
    html = _sans_objet_line(lire("megalis_resultats.html"), 3)
    with pytest.raises(ValueError, match="Bloc de consultation n° 4 illisible"):
        parse_resultats(html)

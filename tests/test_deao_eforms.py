from collections import Counter
from datetime import datetime
from pathlib import Path

from lxml import etree

from src.tasks.deao.schema import AVIS_LOT_SCHEMA
from src.tasks.deao.values import PARIS
from src.tasks.scrap.deao.boamp_joue.boamp.classify import classify
from src.tasks.scrap.deao.boamp_joue.boamp.eforms import map_eforms
from src.tasks.scrap.deao.boamp_joue.xml import direct_child, localname, text

FIXTURES = Path(__file__).parent / "data" / "deao"


def load(nom):
    return etree.parse(FIXTURES / nom).getroot()


def mapper(nom):
    root = load(nom)
    return map_eforms(root, classify(root), Counter())


def nom_organisation(root, org_id):
    for org in root.iter():
        if localname(org.tag) != "Organization":
            continue
        company = direct_child(org, "Company")
        ident = text(direct_child(direct_child(company, "PartyIdentification"), "ID"))
        if ident == org_id:
            return text(direct_child(direct_child(company, "PartyName"), "Name"))
    return None


def test_initial_lot0000():
    rows = mapper("26-87868.xml")
    assert len(rows) == 1
    r = rows[0]
    assert r["lotId"] == "0" and r["referenceAcheteur"] == "2026/4/3/54/00/00/00"
    assert (r["codeCPV"], r["type"], r["procedure"]) == (
        "90520000",
        "Services",
        "Appel d'offres ouvert",
    )
    assert r["dateLimiteReponse"] == datetime(2026, 10, 19, 16, 30, tzinfo=PARIS)
    assert r["urlConsultation"].startswith("https://nr.aws-achat.info")
    assert "&amp;" not in r["urlConsultation"]


def test_rectificatif_date_limite_du_lot():
    r = mapper("26-99074.xml")[0]
    assert r["dateLimiteReponse"] == datetime(2026, 10, 30, 16, 30, tzinfo=PARIS)
    assert "Remise des offres" in r["infosRectificatif"]


def test_eforms_siret_avec_espaces():
    rows = mapper("26-98580.xml")
    assert {r["acheteur_id"] for r in rows} == {"32384135300911"}
    assert [r["lotId"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0]["acheteur_adresse_cp"] == "75019"
    assert rows[0]["techniques"] == ["Accord-cadre"]


def test_pin_mise_en_concurrence():
    rows = mapper("26-98723.xml")
    assert [r["lotId"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0]["codeCPV"] == "66515200"
    assert rows[0]["procedure"] == "Autre procédure"
    assert rows[0]["dateLimiteReponse"] is None


def test_pin_simple_part_et_pin_sans_lot():
    assert [r["lotId"] for r in mapper("26-97189.xml")] == ["1"]
    assert [r["lotId"] for r in mapper("26-94806.xml")] == ["0"]


def test_acheteur_n_est_pas_le_service_provider():
    root = load("26-87868.xml")
    r = map_eforms(root, classify(root), Counter())[0]
    nom_service_provider = nom_organisation(root, "ORG-0001")  # Avenue-Web Systèmes
    assert nom_service_provider is not None
    assert r["acheteur_nom"] == nom_organisation(root, "ORG-0002")
    assert r["acheteur_nom"] != nom_service_provider


def test_memes_cles_que_le_schema():
    for nom in ("26-87868.xml", "26-98580.xml", "26-94806.xml"):
        for r in mapper(nom):
            assert set(r) <= set(AVIS_LOT_SCHEMA)


def test_url_consultation_sans_corruption_des_parametres():
    from src.tasks.scrap.deao.boamp_joue.eforms import _url_consultation

    lot = etree.fromstring(
        "<ProcurementProjectLot><TenderingTerms><CallForTendersDocumentReference>"
        "<Attachment><ExternalReference><URI>https://x/?a=1&amp;region=2&amp;copy=3"
        "</URI></ExternalReference></Attachment>"
        "</CallForTendersDocumentReference></TenderingTerms></ProcurementProjectLot>"
    )
    assert _url_consultation(lot) == "https://x/?a=1&region=2&copy=3"


def _lots_et_ids(root):
    lots = [e for e in root.iter() if localname(e.tag) == "ProcurementProjectLot"]
    ids = [
        next(i for i in lot if localname(i.tag) == "ID" and i.get("schemeName"))
        for lot in lots
    ]
    return lots, ids


def test_lot_sans_id_numerote_tous_les_lots_par_rang():
    root = load("26-98580.xml")
    lots, ids = _lots_et_ids(root)
    for lot in lots[2:]:
        lot.getparent().remove(lot)
    lots[0].remove(ids[0])
    ids[1].text = "LOT-0001"
    rows = map_eforms(root, classify(root), Counter())
    assert [r["lotId"] for r in rows] == ["1", "2"]


def test_ids_de_lot_dupliques_numerotes_par_rang():
    root = load("26-98580.xml")
    _, ids = _lots_et_ids(root)
    for ident in ids:
        ident.text = "LOT-0007"
    rows = map_eforms(root, classify(root), Counter())
    assert [r["lotId"] for r in rows] == ["1", "2", "3", "4"]


def test_unite_de_duree_inconnue_comptee():
    root = load("26-98580.xml")
    for e in root.iter():
        if (
            localname(e.tag) == "DurationMeasure"
            and localname(e.getparent().tag) == "PlannedPeriod"
        ):
            e.set("unitCode", "WEEK")
    compteur = Counter()
    rows = map_eforms(root, classify(root), compteur)
    assert rows[0]["dureeMois"] is None
    assert compteur["non_mappe:unite_duree:WEEK"] >= 1

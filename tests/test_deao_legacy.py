from collections import Counter
from datetime import datetime
from pathlib import Path

from lxml import etree

from src.tasks.deao.schema import AVIS_LOT_SCHEMA
from src.tasks.deao.values import PARIS
from src.tasks.scrap.deao.boamp_joue.boamp.classify import classify
from src.tasks.scrap.deao.boamp_joue.boamp.legacy import map_legacy

FIXTURES = Path(__file__).parent / "data" / "deao"


def load(nom):
    return etree.parse(FIXTURES / nom).getroot()


def mapper(nom):
    root = load(nom)
    return map_legacy(root, classify(root), Counter())


def mapper_xml(corps: str, gestion: str = ""):
    xml = (
        '<jo:ann xmlns:jo="http://boamp.journal-officiel.gouv.fr/XML/3.2.5">'
        f"<DONNEES>{corps}</DONNEES>"
        "<GESTION><REFERENCE><IDWEB>26-1</IDWEB><TYPE_AVIS><NATURE><APPEL_OFFRE/>"
        "</NATURE><STATUT><INITIAL/></STATUT></TYPE_AVIS></REFERENCE>"
        f"<INDEXATION>{gestion}</INDEXATION></GESTION></jo:ann>"
    )
    root = etree.fromstring(xml.encode())
    return map_legacy(root, classify(root), Counter())


def test_fnsimple_initial_lots_sans_numlot():
    rows = mapper("26-92574.xml")
    assert [r["lotId"] for r in rows] == [str(i) for i in range(1, 9)]
    r = rows[0]
    assert r["lotIntitule"].startswith("Installation de chantier")
    assert r["acheteur_id"] == "83942912300020" and r["acheteur_nom"] == "SPL PARISEINE"
    assert (
        r["acheteur_adresse_cp"] == "75004" and r["acheteur_adresse_ville"] == "Paris"
    )
    # le CPV du lot prime sur celui de l'avis (45000000)
    assert (r["codeCPV"], r["dureeMois"], r["type"], r["procedure"]) == (
        "45262660",
        30,
        "Travaux",
        "Appel d'offres ouvert",
    )
    assert r["lieuExecution_typeCode"] is None
    assert r["dateLimiteReponse"] == datetime(2026, 10, 22, 12, 0, tzinfo=PARIS)
    assert r["objet"].startswith("Travaux de réhabilitation de la tribune")
    assert set(r) <= set(AVIS_LOT_SCHEMA)
    assert r["urlConsultation"].startswith("https://marches.maximilien.fr/")
    assert r["techniques"] == []


def test_fnsimple_lots_avec_montant_et_sad():
    rows = mapper("26-99177.xml")
    assert len(rows) == 11
    assert rows[0]["montantEstime"] == 36000.0
    assert rows[0]["codeCPV"] == "98330000"
    assert rows[0]["dureeMois"] == 24
    assert rows[0]["techniques"] == ["Système d'acquisition dynamique"]


def test_mapa_sans_siret_sans_lot():
    rows = mapper("26-99870.xml")
    assert len(rows) == 1 and rows[0]["lotId"] == "0"
    r = rows[0]
    assert r["acheteur_id"] is None
    assert r["acheteur_nom"] == "Communauté Lesneven Côte des Légendes"
    assert r["acheteur_adresse_cp"] == "29260"
    assert r["acheteur_adresse_voie"] == "12 Bvd des Frères Lumière"
    assert r["procedure"] == "Procédure adaptée"
    assert r["dateLimiteReponse"] == datetime(2026, 11, 19, 11, 30, tzinfo=PARIS)
    assert r["dureeMois"] == 29
    assert r["lieuExecution_code"] == "29260"
    assert r["lieuExecution_typeCode"] == "Code postal"
    assert r["description"].startswith("Pilotage et rédaction")
    assert r["urlConsultation"].startswith("https://marches.megalis.bretagne.bzh/")
    assert "26-99870" in r["urlAvis"]


def test_rectificatif_une_ligne_avec_texte():
    rows = mapper("26-99891.xml")
    assert len(rows) == 1 and rows[0]["lotId"] == "0"
    assert rows[0]["infosRectificatif"].startswith(
        "La date limite de remise des plis est reportée"
    )


def test_annulation_texte():
    rows = mapper("26-99542.xml")
    assert len(rows) == 1 and rows[0]["lotId"] == "0"
    assert rows[0]["infosRectificatif"].startswith("Publié via un mauvais canal")


def test_numlot_en_double_bascule_sur_le_rang():
    rows = mapper_xml(
        "<FNSimple><organisme><nomOfficiel>X</nomOfficiel></organisme><initial><lots>"
        "<lot><numLot>1</numLot><intitule>A</intitule></lot>"
        "<lot><numLot>1</numLot><intitule>B</intitule></lot>"
        "</lots></initial></FNSimple>"
    )
    assert [r["lotId"] for r in rows] == ["1", "2"]
    assert [r["lotIntitule"] for r in rows] == ["A", "B"]


def test_numlot_distincts_normalises():
    rows = mapper_xml(
        "<FNSimple><organisme/><initial><lots>"
        "<lot><numLot>02</numLot></lot><lot><numLot>1</numLot></lot>"
        "</lots></initial></FNSimple>"
    )
    assert [r["lotId"] for r in rows] == ["2", "1"]


def test_champs_absents_des_fixtures():
    rows = mapper_xml(
        "<FNSimple><organisme><nomOfficiel>X</nomOfficiel>"
        "<typeIdentificationNational><siret/></typeIdentificationNational>"
        "<codeIdentificationNational>839 429 123 00020</codeIdentificationNational>"
        "<cp>78 140</cp></organisme><initial>"
        "<natureMarche><valeurEstimee><valeur>1234,5</valeur></valeurEstimee>"
        "<duree><nbJours>61</nbJours></duree>"
        "<numeroReference>REF-1</numeroReference>"
        "<lieuExecution><cp>78 140</cp></lieuExecution>"
        "<techAchat><accordCadre/></techAchat></natureMarche>"
        "<adressesComplt><document><coord><url>https://doc.example</url></coord>"
        "</document></adressesComplt></initial></FNSimple>"
    )
    (r,) = rows
    assert r["montantEstime"] == 1234.5
    assert r["dureeMois"] == 3
    assert r["referenceAcheteur"] == "REF-1"
    assert r["acheteur_adresse_cp"] == "78140"
    assert r["lieuExecution_code"] == "78140"
    assert r["techniques"] == ["Accord-cadre"]
    assert r["urlConsultation"] == "https://doc.example"
    assert r["lotId"] == "0"


def test_fourchette_seule_montant_nul():
    (r,) = mapper_xml(
        "<FNSimple><organisme/><initial><natureMarche><valeurEstimee><fourchette>"
        "<valeurBasse>1</valeurBasse><valeurHaute>2</valeurHaute></fourchette>"
        "</valeurEstimee></natureMarche></initial></FNSimple>"
    )
    assert r["montantEstime"] is None

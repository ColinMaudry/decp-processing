from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from src.tasks.scrap.deao.aws.lignes import (
    departement,
    libelle_procedure_aws,
    lignes_avis,
)
from src.tasks.scrap.deao.aws.parse import AvisListe, parse_avis

DOSSIER = Path(__file__).parent / "data" / "aws"
CODE = "www.marches-publics.info"
BASE = "https://www.marches-publics.info"


def avis_fixture(nom):
    return parse_avis((DOSSIER / f"avis_{nom}.html").read_text(encoding="utf-8"))


def liste_fixture(avis, **kw):
    from datetime import date

    base = AvisListe(
        idm="2026260690",
        numero_avis="123456",
        chemin_avis="/Annonces/MPI-pub-123456.htm",
        acheteur_nom="Acheteur liste",
        code_postal="13006",
        reference="REF-LISTE",
        objet="Objet liste",
        nombre_lots=len(avis.lots),
        date_publication=date(2026, 11, 10),
        date_limite=None,
        nature="Services",
        considerations_sociales=False,
        considerations_environnementales=False,
    )
    return replace(base, **kw)


def test_avis_alloti():
    avis = avis_fixture("2026314252")
    liste = liste_fixture(avis, idm="2026314252", nombre_lots=3)
    lignes = lignes_avis(CODE, BASE, liste, avis, "https://rc")
    assert [ligne["uid"] for ligne in lignes] == [
        f"{CODE}_2026314252_{lot.numero}" for lot in avis.lots
    ]
    assert len(lignes) == 3
    assert lignes[0]["codeCPV"] == avis.lots[0].code_cpv
    assert lignes[0]["lotIntitule"] == "Voiries"
    assert lignes[0]["lieuExecution_code"] == "25"
    assert lignes[0]["lieuExecution_typeCode"] == "Code département"
    assert lignes[0]["procedure"] == "Procédure adaptée"
    assert lignes[0]["techniques"] == ["Accord-cadre"]
    assert lignes[0]["dureeMois"] == 24
    assert lignes[0]["acheteur_id"] == avis.siret
    assert lignes[0]["id"] == "2026314252"
    assert lignes[0]["idDernierAvis"] == "123456"
    assert lignes[0]["urlConsultation"] == BASE + "/Annonces/MPI-pub-123456.htm"
    assert lignes[0]["urlDce"] is None
    assert lignes[0]["urlReglementConsultation"] == "https://rc"
    assert lignes[0]["rectifie"] is False
    assert lignes[0]["avisLies"] == [] and lignes[0]["sourceFile"] is None
    assert lignes[0]["sourceDataset"] == CODE


def test_avis_non_alloti():
    avis = avis_fixture("2026275758")
    liste = liste_fixture(avis, idm="2026275758")
    (ligne,) = lignes_avis(CODE, BASE, liste, avis, "https://rc")
    assert ligne["lotId"] == "0" and ligne["uid"] == f"{CODE}_2026275758_0"
    assert ligne["codeCPV"] == avis.cpv_principal
    assert ligne["procedure"] == "Appel d'offres ouvert"
    assert ligne["variantes"] == "interdites"
    assert ligne["lieuExecution_code"] == "25"


def test_procedure_inconnue():
    stats = Counter()
    assert libelle_procedure_aws("Truc inédit", stats) == "Autre procédure"
    assert stats["procedure_inconnue:Truc inédit"] == 1
    assert libelle_procedure_aws("Procédure adaptée ouverte") == "Procédure adaptée"
    assert libelle_procedure_aws(None, stats) == "Autre procédure"
    assert len(stats) == 1


@pytest.mark.parametrize(
    "liste_v, avis_v, attendu",
    [
        (True, None, True),
        (False, False, False),
        (False, None, None),
        (False, True, True),
    ],
)
def test_considerations(liste_v, avis_v, attendu):
    avis = replace(
        avis_fixture("2026275758"),
        considerations_environnementales=avis_v,
        considerations_sociales=avis_v,
    )
    liste = liste_fixture(
        avis,
        considerations_environnementales=liste_v,
        considerations_sociales=liste_v,
    )
    (ligne,) = lignes_avis(CODE, BASE, liste, avis, "")
    assert ligne["considerationsEnvironnementales"] is attendu
    assert ligne["considerationsSociales"] is attendu


def test_lieu_repli_et_replis_liste():
    avis = replace(
        avis_fixture("2026275758"),
        adresse_cp=None,
        acheteur_nom="",
        reference=None,
        objet="",
        date_envoi=None,
        date_limite=None,
    )
    liste = liste_fixture(avis, code_postal="97411")
    (ligne,) = lignes_avis(CODE, BASE, liste, avis, "")
    assert ligne["lieuExecution_code"] == "974"
    assert ligne["acheteur_nom"] == "Acheteur liste"
    assert ligne["referenceAcheteur"] == "REF-LISTE"
    assert ligne["objet"] == "Objet liste"
    assert ligne["datePublicationInitiale"] == liste.date_publication


def test_departement():
    assert departement("25400") == "25"
    assert departement("97411") == "974"
    assert departement("20000") == "2A"
    assert departement("20200") == "2B"
    assert departement(None) is None and departement("abc") is None

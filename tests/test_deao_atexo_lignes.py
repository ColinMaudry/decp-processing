from collections import Counter
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

from src.tasks.scrap.deao.atexo.lignes import (
    PROCEDURES,
    lignes_consultation,
    lots_depuis_precedent,
)
from src.tasks.scrap.deao.atexo.parse import ConsultationListe, DetailConsultation, Lot

BASE = "https://marches.megalis.bretagne.bzh"


def _consultation(**kw) -> ConsultationListe:
    defaut = dict(
        ref_cons="242752",
        org_cons="e3x",
        procedure_code="MAPA",
        categorie="Services",
        date_mise_en_ligne=date(2026, 11, 2),
        reference="2026-01",
        intitule="Intitulé",
        objet="Objet détaillé",
        organisme="Commune de Test",
        code_postal="35000",
        ville="Rennes",
        nombre_lots=2,
        lieux_execution=["35", "22"],
        date_limite=datetime(2026, 12, 1, 12, 0, tzinfo=ZoneInfo("Europe/Paris")),
    )
    return ConsultationListe(**{**defaut, **kw})


DETAIL = DetailConsultation("45000000", None, None, "72,09 Mo")


def test_consultation_allotie_une_ligne_par_lot():
    lots = [
        Lot("1", "Lot un", "Travaux", None, "45100000"),
        Lot("2", "Lot deux", None, None, None),
    ]
    lignes = lignes_consultation(
        "megalis_bretagne", BASE, _consultation(), DETAIL, lots, None
    )
    assert [x["uid"] for x in lignes] == [
        "megalis_bretagne_242752_1",
        "megalis_bretagne_242752_2",
    ]
    assert [x["codeCPV"] for x in lignes] == ["45100000", "45000000"]
    assert [x["type"] for x in lignes] == ["Travaux", "Services"]
    assert lignes[0]["lotIntitule"] == "Lot un"


def test_consultation_non_allotie():
    lignes = lignes_consultation(
        "megalis_bretagne",
        BASE,
        _consultation(nombre_lots=0),
        DETAIL,
        [],
        "https://s3/x.zip",
    )
    assert len(lignes) == 1
    ligne = lignes[0]
    assert ligne["lotId"] == "0"
    assert ligne["uid"] == "megalis_bretagne_242752_0"
    assert ligne["codeCPV"] == "45000000"
    assert ligne["id"] == "242752"
    assert ligne["idDernierAvis"] is None
    assert ligne["objet"] == "Intitulé"
    assert ligne["description"] == "Objet détaillé"
    assert ligne["lieuExecution_code"] == "35"
    assert ligne["lieuExecution_typeCode"] == "Code département"
    assert ligne["acheteur_pays"] == "FRA"
    assert ligne["acheteur_adresse_cp"] == "35000"
    assert ligne["urlDce"] == "https://s3/x.zip"
    assert ligne["sourceDataset"] == "megalis_bretagne"


def test_urls():
    ligne = lignes_consultation(
        "megalis_bretagne", BASE, _consultation(), None, [], None
    )[0]
    assert (
        ligne["urlConsultation"]
        == f"{BASE}/entreprise/consultation/242752?orgAcronyme=e3x"
    )
    assert ligne["urlReglementConsultation"] == (
        f"{BASE}/index.php?page=Entreprise.EntrepriseDownloadReglement"
        "&id=MjQyNzUy&orgAcronyme=e3x"
    )


def test_procedure_mapa():
    ligne = lignes_consultation("m", BASE, _consultation(), None, [], None)[0]
    assert ligne["procedure"] == "Procédure adaptée"


def test_procedures_observees_et_inconnues():
    stats = Counter()
    for code, attendu in [
        ("AOO", "Appel d'offres ouvert"),
        ("PAN", "Procédure négociée"),
        ("SAD-A", "Autre procédure"),
        ("XYZ", "Autre procédure"),
    ]:
        ligne = lignes_consultation(
            "m", BASE, _consultation(procedure_code=code), None, [], None, stats
        )[0]
        assert ligne["procedure"] == attendu
    assert stats == Counter({"procedure_inconnue:XYZ": 1})
    assert PROCEDURES["AOR"] == "Appel d'offres restreint"


def test_lots_depuis_precedent():
    lots = [Lot("10", "Dix", "Travaux", None, "45"), Lot("2", "Deux", None, None, None)]
    lignes = lignes_consultation("m", BASE, _consultation(), None, lots, None)
    precedent = pl.DataFrame(
        lignes
        + lignes_consultation(
            "m", BASE, _consultation(ref_cons="9", nombre_lots=0), None, [], None
        )
    )
    relus = lots_depuis_precedent(precedent, "242752")
    assert [lot.numero for lot in relus] == ["2", "10"]
    assert relus[1].code_cpv == "45" and relus[1].intitule == "Dix"
    assert lots_depuis_precedent(precedent, "9") == []


def test_sans_lieu_execution_pas_de_type_de_code():
    ligne = lignes_consultation(
        "megalis_bretagne", BASE, _consultation(lieux_execution=[]), DETAIL, [], None
    )[0]
    assert ligne["lieuExecution_code"] is None
    assert ligne["lieuExecution_typeCode"] is None

from collections import Counter
from pathlib import Path

from lxml import etree

from src.tasks.deao.schema import AVIS_LOT_SCHEMA
from src.tasks.deao.values import departement_depuis_nuts
from src.tasks.scrap.deao.boamp_joue.joue.classify import classify_joue
from src.tasks.scrap.deao.boamp_joue.joue.parse import parse_avis, parse_day
from src.tasks.scrap.deao.boamp_joue.joue.source import JOUE

DOSSIER = Path(__file__).parent / "data" / "deao" / "joue"


def _xml(pn: str) -> bytes:
    return (DOSSIER / f"{pn}.xml").read_bytes()


def _cl(pn: str):
    c = classify_joue(etree.fromstring(_xml(pn)))
    return c.typeAvis, c.preInformation, c.retenu


def _parse(pn: str) -> list[dict]:
    return parse_avis(_xml(pn), f"{pn}.xml", Counter())


def test_classement():
    assert _cl("677195-2026") == ("initial", False, True)
    assert _cl("706679-2026") == ("rectificatif", None, True)
    assert _cl("603380-2026") == ("annulation", None, True)
    assert _cl("680725-2026") == ("initial", True, True)
    assert _cl("700680-2026")[2] is False


def test_parse_rectificatif_seminor():
    lignes = _parse("706679-2026")
    assert {x["idAvis"] for x in lignes} == {"706679-2026"}
    assert lignes[0]["idEformsPrecedent"] == "c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"
    assert lignes[0]["avisPrecedent"] is None and lignes[0]["sourceDataset"] == "joue"
    assert (
        lignes[0]["urlAvis"] == "https://ted.europa.eu/fr/notice/-/detail/706679-2026"
    )


def test_parse_acheteur_etranger():
    ligne = _parse("708669-2026")[0]
    assert ligne["acheteur_pays"] == "POL"
    assert ligne["lieuExecution_code"] == departement_depuis_nuts("FRK26")


def test_parse_day_compteurs():
    df, c = parse_day({p.name: p.read_bytes() for p in DOSSIER.glob("*.xml")})
    assert c["exclu:result"] == 1 and c["retenu"] == 9 and c["erreur_mapping"] == 0
    assert df.schema == AVIS_LOT_SCHEMA


def test_source():
    assert JOUE.nom == "joue"


def test_avis_sans_type_compte_exclu_sans_type():
    from collections import Counter

    from src.tasks.scrap.deao.boamp_joue.joue.parse import parse_avis

    c = Counter()
    assert parse_avis(b"<Notice/>", "x.xml", c) == []
    assert c["exclu:sans_type"] == 1 and "exclu:None" not in c

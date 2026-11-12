from collections import Counter
from pathlib import Path

from lxml import etree

from src.tasks.deao.eforms import (
    change_reason,
    changed_notice_id,
    map_notice,
    notice_id,
)
from src.tasks.deao.sources.boamp.classify import classify
from src.tasks.deao.sources.boamp.eforms import map_eforms
from src.tasks.deao.xml import child_fr, find, text

FIXTURES = Path(__file__).parent / "data" / "deao"


def _root(nom):
    return etree.parse(FIXTURES / nom).getroot()


def _notice(nom):
    eforms = find(_root(nom), "EFORMS")
    return next(c for c in eforms if isinstance(c.tag, str))


def test_child_fr_prefere_le_francais():
    e = etree.fromstring(
        '<r xmlns:cbc="urn:c"><cbc:Name languageID="ENG">A</cbc:Name>'
        '<cbc:Name languageID="FRA">B</cbc:Name></r>'
    )
    assert text(child_fr(e, "Name")) == "B"


def test_child_fr_premier_sinon():
    e = etree.fromstring(
        '<r><Name languageID="POL">A</Name><Name languageID="DEU">B</Name></r>'
    )
    assert text(child_fr(e, "Name")) == "A"
    assert child_fr(e, "Autre") is None


def test_notice_id_et_changed_notice_id_boamp_26_99074():
    notice = _notice("26-99074.xml")
    assert notice_id(notice) == "53d43f07-bbcd-4221-a838-ede3e3fe50b8"
    assert changed_notice_id(notice) == "17114689-cb66-44e1-b420-be7e501a33b0"
    assert changed_notice_id(_notice("26-87868.xml")) is None


def test_changed_notice_id_retire_le_suffixe_de_version():
    n = etree.fromstring(
        '<N xmlns:efbc="urn:e"><x><efbc:ChangedNoticeIdentifier>'
        "ABCDEF01-1234-1234-1234-123456789012-01</efbc:ChangedNoticeIdentifier></x></N>"
    )
    assert changed_notice_id(n) == "abcdef01-1234-1234-1234-123456789012"


def test_map_notice_identique_a_map_eforms_hors_url():
    for chemin in sorted(FIXTURES.glob("*.xml")):
        root = _root(chemin.name)
        classement = classify(root)
        if classement.formatSource != "eforms" or not classement.retenu:
            continue
        lignes = map_eforms(root, classement, Counter())
        notice = _notice(chemin.name)
        communes = map_notice(notice, Counter())
        assert len(lignes) == len(communes), chemin.name
        for ligne, commune in zip(lignes, communes, strict=True):
            ligne = dict(ligne)
            url = ligne.pop("urlAvis")
            assert url
            # surcharges BOAMP : type, procedure (INDEXATION), dateLimiteReponse
            for cle in ("type", "procedure", "dateLimiteReponse"):
                ligne.pop(cle)
                commune.pop(cle)
            assert ligne == commune, chemin.name


def test_map_notice_acheteur_pays():
    lignes = map_notice(_notice("26-87868.xml"), Counter())
    assert {ligne["acheteur_pays"] for ligne in lignes} == {"FRA"}


def test_change_reason():
    n = etree.fromstring(
        '<N xmlns:cbc="urn:c"><x><cbc:ReasonCode listName="accelerated-procedure">'
        'false</cbc:ReasonCode><cbc:ReasonCode listName="change-corrig-justification">'
        "cor-buy</cbc:ReasonCode></x></N>"
    )
    assert change_reason(n) == "cor-buy"
    assert change_reason(_notice("26-87868.xml")) is None


def test_changed_notice_id_uuid_nu_a_dernier_groupe_numerique():
    uuid = "12345678-1234-1234-1234-123456789012"
    for valeur, attendu in ((uuid, uuid), (f"{uuid}-01", uuid)):
        n = etree.fromstring(
            '<N xmlns:efbc="urn:e"><x><efbc:ChangedNoticeIdentifier>'
            f"{valeur}</efbc:ChangedNoticeIdentifier></x></N>"
        )
        assert changed_notice_id(n) == attendu

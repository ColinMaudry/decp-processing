"""Tests pour les définitions de champs BOAMP (#178)."""

from pathlib import Path

from src.tasks.boamp.field_definitions import build_legacy_index, normalize_xpath


def test_normalize_xpath_simple():
    assert normalize_xpath("/*/cbc:RegulatoryDomain") == ("RegulatoryDomain",)


def test_normalize_xpath_strips_prefixes_and_predicates():
    xp = "/*/cac:TenderingTerms/cac:ProcurementLegislationDocumentReference[cbc:ID/text()='LocalLegalBasis']/cbc:DocumentDescription"
    assert normalize_xpath(xp) == (
        "TenderingTerms",
        "ProcurementLegislationDocumentReference",
        "DocumentDescription",
    )


def test_normalize_xpath_drops_attribute_steps():
    assert normalize_xpath("/*/cbc:NoticeTypeCode/@listName") == ("NoticeTypeCode",)


_MINI_XSD = """<?xml version="1.0"?>
<schema xmlns="http://www.w3.org/2001/XMLSchema">
  <element name="cp">
    <annotation><documentation>Code postal</documentation></annotation>
  </element>
  <element name="objet">
    <annotation><documentation>- Objet du marché</documentation></annotation>
  </element>
  <element name="objet">
    <annotation><documentation>Objet du marché</documentation></annotation>
  </element>
  <element name="sansdoc"/>
</schema>
"""


def test_build_legacy_index(tmp_path: Path):
    xsd = tmp_path / "mini.xsd"
    xsd.write_text(_MINI_XSD, encoding="utf-8")
    idx = build_legacy_index([xsd])
    assert idx["cp"] == "Code postal"
    # "- Objet du marché" et "Objet du marché" -> même texte après nettoyage -> dédoublonné
    assert idx["objet"] == "Objet du marché"
    assert "sansdoc" not in idx

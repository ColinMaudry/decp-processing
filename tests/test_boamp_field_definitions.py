"""Tests pour les définitions de champs BOAMP (#178)."""

from src.tasks.boamp.field_definitions import normalize_xpath


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

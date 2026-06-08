"""Tests pour les définitions de champs BOAMP (#178)."""

import json
from pathlib import Path

from src.tasks.boamp.field_definitions import (
    build_eforms_index,
    build_legacy_index,
    column_to_ubl_path,
    is_eforms,
    normalize_xpath,
    resolve_column,
)


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


_FIELD_FR = """<?xml version="1.0" encoding="UTF-8"?>
<properties>
  <entry key="field|description|BT-01-notice">Base juridique de la procedure.</entry>
  <entry key="field|description|BT-02-notice">Type de l'avis.</entry>
  <entry key="field|name|BT-02-notice">Notice Type</entry>
</properties>
"""

_FIELDS_JSON = {
    "fields": [
        {
            "id": "BT-01-notice",
            "name": "Procedure Legal Basis",
            "xpathAbsolute": "/*/cbc:RegulatoryDomain",
        },
        {
            "id": "BT-02-notice",
            "name": "Notice Type",
            "xpathAbsolute": "/*/cbc:NoticeTypeCode",
        },
        {
            "id": "BT-03-notice",
            "name": "Form Type",
            "xpathAbsolute": "/*/cbc:NoticeTypeCode/@listName",
        },
        {"id": "BT-99-x", "name": "Sans FR", "xpathAbsolute": "/*/cac:Foo/cbc:Bar"},
    ]
}


def test_build_eforms_index(tmp_path):
    (tmp_path / "field_fr.xml").write_text(_FIELD_FR, encoding="utf-8")
    (tmp_path / "fields.json").write_text(json.dumps(_FIELDS_JSON), encoding="utf-8")
    idx = build_eforms_index(tmp_path)
    assert idx[("RegulatoryDomain",)] == "Base juridique de la procedure."
    assert idx[("NoticeTypeCode",)] == "Type de l'avis."
    # champ d'attribut ignoré : BT-03 (@listName) ne crée pas d'entrée propre
    # champ sans FR -> fallback sur le name EN
    assert idx[("Foo", "Bar")] == "Sans FR"


def test_is_eforms():
    assert is_eforms("ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain")
    assert is_eforms("ProcurementProjectLot_TenderingTerms_FundingProgramCode")
    assert not is_eforms("ann_DONNEES_DSP_initial_objet")


def test_column_to_ubl_path_notice_level():
    col = "ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain"
    assert column_to_ubl_path(col) == ("RegulatoryDomain",)


def test_column_to_ubl_path_lot_level():
    col = "ProcurementProjectLot_TenderingTerms_FundingProgramCode"
    assert column_to_ubl_path(col) == (
        "ProcurementProjectLot",
        "TenderingTerms",
        "FundingProgramCode",
    )


_NOT_FOUND = "(définition introuvable)"


def test_resolve_column_legacy():
    legacy = {
        "DSP": "Délégation de service public",
        "adr": "Informations d'adresse",
        "cp": "Code postal",
    }
    eforms = {}
    col = "ann_DONNEES_DSP_adr_cp"
    defs, leaf = resolve_column(col, legacy, eforms)
    # 'ann' et 'DONNEES' omis (structurels)
    assert defs == (
        "DSP : Délégation de service public\n"
        "adr : Informations d'adresse\n"
        "cp : Code postal"
    )
    assert leaf == "Code postal"


def test_resolve_column_legacy_missing_segment():
    legacy = {"cp": "Code postal"}
    defs, leaf = resolve_column("ann_DONNEES_DSP_cp", legacy, {})
    assert defs == f"DSP : {_NOT_FOUND}\ncp : Code postal"
    assert leaf == "Code postal"


def test_resolve_column_eforms_leaf_resolved():
    eforms = {("RegulatoryDomain",): "Base juridique de la procedure."}
    col = "ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain"
    defs, leaf = resolve_column(col, {}, eforms)
    # seuls les segments non-structurels restent : RegulatoryDomain (la feuille)
    assert defs == "RegulatoryDomain : Base juridique de la procedure."
    assert leaf == "Base juridique de la procedure."


def test_resolve_column_eforms_intermediate_best_effort():
    eforms = {
        (
            "UBLExtensions",
            "UBLExtension",
            "ExtensionContent",
            "EformsExtension",
            "Organizations",
            "Organization",
            "Company",
            "PostalAddress",
            "CityName",
        ): "Ville de l'organisation.",
    }
    col = (
        "ann_DONNEES_EFORMS_ContractNotice_UBLExtensions_UBLExtension_"
        "ExtensionContent_EformsExtension_Organizations_Organization_Company_"
        "PostalAddress_CityName"
    )
    defs, leaf = resolve_column(col, {}, eforms)
    # wrappers structurels omis ; intermédiaires non-feuille -> introuvable ; feuille OK
    assert defs == (
        f"Organizations : {_NOT_FOUND}\n"
        f"Organization : {_NOT_FOUND}\n"
        f"Company : {_NOT_FOUND}\n"
        f"PostalAddress : {_NOT_FOUND}\n"
        "CityName : Ville de l'organisation."
    )
    assert leaf == "Ville de l'organisation."

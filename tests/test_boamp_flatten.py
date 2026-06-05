from lxml import etree

from src.tasks.boamp.flatten import (
    detect_format,
    flatten,
    is_included,
    localname,
    parse_type_avis,
)


def _el(xml: str):
    return etree.fromstring(xml.encode("utf-8"))


def test_localname_strips_namespace():
    el = _el('<a xmlns="urn:x"><b>1</b></a>')
    assert localname(el.tag) == "a"


def test_flatten_single_leaf_is_length_one_list():
    el = _el("<root><a>x</a></root>")
    assert flatten(el) == {"root_a": ["x"]}


def test_flatten_repeated_leaf_accumulates():
    el = _el("<root><a>x</a><a>y</a></root>")
    assert flatten(el) == {"root_a": ["x", "y"]}


def test_flatten_nested_paths_and_skips_empty_text():
    el = _el("<root><g><a>x</a><b></b></g></root>")
    assert flatten(el) == {"root_g_a": ["x"]}


def test_flatten_skip_localnames():
    el = _el("<root><keep>1</keep><HTML>big</HTML></root>")
    assert flatten(el, skip_localnames={"HTML"}) == {"root_keep": ["1"]}


def test_flatten_start_path_override():
    el = _el("<lot><numLot>4</numLot></lot>")
    assert flatten(el, start_path="lot") == {"lot_numLot": ["4"]}


ANN_TPL = """
<ann>
  <DONNEES>{donnees}</DONNEES>
  <GESTION><REFERENCE><IDWEB>26-1</IDWEB>
    <TYPE_AVIS>
      <FAMILLE><{famille}/></FAMILLE>
      <NATURE>{nature}</NATURE>
      <STATUT><{statut}/></STATUT>
    </TYPE_AVIS>
  </REFERENCE></GESTION>
</ann>
"""


def _ann(donnees, nature_xml, famille="MAPA", statut="INITIAL"):
    return _el(
        ANN_TPL.format(
            donnees=donnees, nature=nature_xml, famille=famille, statut=statut
        )
    )


def test_detect_format_eforms_vs_legacy():
    assert detect_format(_ann("<EFORMS/>", "<APPEL_OFFRE/>")) == "eforms"
    assert detect_format(_ann("<MAPA/>", "<APPEL_OFFRE/>")) == "boamp"


def test_parse_type_avis_returns_nature_sub_statut_famille():
    nature, sub, statut, famille = parse_type_avis(
        _ann("<MAPA/>", "<APPEL_OFFRE><STANDARD/></APPEL_OFFRE>")
    )
    assert (nature, sub, statut, famille) == (
        "APPEL_OFFRE",
        "STANDARD",
        "INITIAL",
        "MAPA",
    )


def test_is_included_whitelist():
    assert is_included("APPEL_OFFRE", None) is True
    assert is_included("QUALIFICATION", "STANDARD") is True
    assert is_included("ATTRIBUTION", "STANDARD") is False
    assert is_included("PROFIL_ACHETEUR", None) is False


def test_is_included_pre_information_only_when_mise_en_concurrence():
    assert is_included("PRE-INFORMATION", "MISE_EN_CONCURRENCE") is True
    assert is_included("PERIODIQUE", "MISE_EN_CONCURRENCE") is True
    assert is_included("PRE-INFORMATION", "STANDARD") is False
    assert is_included("PERIODIQUE", None) is False


EFORMS_LOT = """
<ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b">
  <cac:ProcurementProjectLot>
    <cbc:ID schemeName="Lot">LOT-0001</cbc:ID>
    <cac:ProcurementProject><cbc:Name>Intitulé A</cbc:Name></cac:ProcurementProject>
  </cac:ProcurementProjectLot>
  <cac:ProcurementProjectLot>
    <cbc:ID schemeName="Lot">LOT-0002</cbc:ID>
    <cac:ProcurementProject><cbc:Name>Intitulé B</cbc:Name></cac:ProcurementProject>
  </cac:ProcurementProjectLot>
</ContractNotice>
"""

LEGACY_LOTS = """
<MAPA><initial><lots>
  <lot><numLot>4</numLot><description>Métal</description></lot>
  <lot><numLot>5</numLot><description>Bois</description></lot>
</lots></initial></MAPA>
"""


def test_find_lots_eforms():
    from src.tasks.boamp.flatten import find_lots

    lots = find_lots(_el(EFORMS_LOT), "eforms")
    assert len(lots) == 2


def test_find_lots_legacy():
    from src.tasks.boamp.flatten import find_lots

    lots = find_lots(_el(LEGACY_LOTS), "boamp")
    assert len(lots) == 2


def test_extract_lot_core_eforms():
    from src.tasks.boamp.flatten import extract_lot_core, find_lots

    lots = find_lots(_el(EFORMS_LOT), "eforms")
    assert extract_lot_core(lots[0], "eforms") == {
        "lot_id": "LOT-0001",
        "lot_intitule": "Intitulé A",
    }


def test_extract_lot_core_legacy():
    from src.tasks.boamp.flatten import extract_lot_core, find_lots

    lots = find_lots(_el(LEGACY_LOTS), "boamp")
    assert extract_lot_core(lots[1], "boamp") == {
        "lot_id": "5",
        "lot_intitule": "Bois",
    }


EFORMS_BUYER = """
<ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b" xmlns:efac="urn:e">
  <ext:UBLExtensions xmlns:ext="urn:x"><efac:Organizations>
    <efac:Organization><efac:Company>
      <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0001</cbc:ID></cac:PartyIdentification>
      <cac:PartyName><cbc:Name>Hauts-de-Seine Habitat</cbc:Name></cac:PartyName>
      <cac:PostalAddress><cbc:CityName>Levallois</cbc:CityName></cac:PostalAddress>
      <cac:PartyLegalEntity><cbc:CompanyID>27920022400012</cbc:CompanyID></cac:PartyLegalEntity>
    </efac:Company></efac:Organization>
    <efac:Organization><efac:Company>
      <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0000</cbc:ID></cac:PartyIdentification>
      <cac:PartyName><cbc:Name>Plateforme eSender</cbc:Name></cac:PartyName>
    </efac:Company></efac:Organization>
  </efac:Organizations></ext:UBLExtensions>
  <cac:ContractingParty><cac:Party>
    <cac:PartyIdentification><cbc:ID>ORG-0001</cbc:ID></cac:PartyIdentification>
    <cac:ServiceProviderParty><cac:Party>
      <cac:PartyIdentification><cbc:ID>ORG-0000</cbc:ID></cac:PartyIdentification>
    </cac:Party></cac:ServiceProviderParty>
  </cac:Party></cac:ContractingParty>
</ContractNotice>
"""


def test_eforms_acheteur_resolves_buyer_not_service_provider():
    from src.tasks.boamp.flatten import eforms_acheteur

    res = eforms_acheteur(_el(EFORMS_BUYER))
    assert res == {
        "acheteur_nom": "Hauts-de-Seine Habitat",
        "acheteur_siret": "27920022400012",
        "acheteur_ville": "Levallois",
    }


def test_eforms_acheteur_empty_when_no_contracting_party():
    from src.tasks.boamp.flatten import eforms_acheteur

    res = eforms_acheteur(_el('<ContractNotice xmlns:cac="urn:c"/>'))
    assert res == {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}


LEGACY_FULL = """
<ann>
  <DONNEES><MAPA><organisme>
      <acheteurPublic>Ville de X</acheteurPublic>
      <adr><ville>Paristown</ville></adr>
    </organisme>
    <initial><description><objet>Travaux divers</objet></description></initial>
  </MAPA></DONNEES>
  <GESTION><REFERENCE><IDWEB>26-10979</IDWEB>
    <TYPE_AVIS><FAMILLE><MAPA/></FAMILLE><NATURE><APPEL_OFFRE><STANDARD/></APPEL_OFFRE></NATURE>
      <STATUT><INITIAL/></STATUT></TYPE_AVIS>
  </REFERENCE>
  <INDEXATION><DATE_PUBLICATION>2026-01-31</DATE_PUBLICATION>
    <DATE_LIMITE_REPONSE>2026-03-30T22:00:00</DATE_LIMITE_REPONSE></INDEXATION>
  </GESTION>
</ann>
"""


def test_extract_core_legacy():
    from src.tasks.boamp.flatten import extract_core

    root = _el(LEGACY_FULL)
    core = extract_core(root, "boamp", "26-10979.xml")
    assert core["id_annonce"] == "bo_26-10979"
    assert core["source_format"] == "boamp"
    assert core["nature_avis"] == "APPEL_OFFRE"
    assert core["statut_avis"] == "INITIAL"
    assert core["famille_avis"] == "MAPA"
    assert core["type_document"] == "MAPA"
    assert core["sous_type_eforms"] is None
    assert core["acheteur_nom"] == "Ville de X"
    assert core["acheteur_ville"] == "Paristown"
    assert core["objet"] == "Travaux divers"
    assert core["date_publication"] == "2026-01-31"
    assert core["date_limite_reponse"] == "2026-03-30T22:00:00"
    assert core["url_avis"] == 'https://www.boamp.fr/pages/avis/?q=idweb:"26-10979"'
    assert core["fichier_source"] == "26-10979.xml"
    assert core["lot_id"] is None and core["lot_intitule"] is None


def test_extract_core_eforms_id_prefix_and_subtype():
    from src.tasks.boamp.flatten import extract_core

    xml = """
    <ann><DONNEES><EFORMS><ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b" xmlns:efac="urn:e" xmlns:efbc="urn:f">
        <ext:UBLExtensions xmlns:ext="urn:x"><efac:NoticeSubType>
          <cbc:SubTypeCode listName="notice-subtype">16</cbc:SubTypeCode></efac:NoticeSubType></ext:UBLExtensions>
        <cac:ProcurementProject><cbc:Name>Objet global</cbc:Name></cac:ProcurementProject>
      </ContractNotice></EFORMS></DONNEES>
      <GESTION><REFERENCE><IDWEB>26-100</IDWEB>
        <TYPE_AVIS><FAMILLE><JOUE/></FAMILLE><NATURE><APPEL_OFFRE/></NATURE><STATUT><INITIAL/></STATUT></TYPE_AVIS>
      </REFERENCE><INDEXATION><DATE_PUBLICATION>2026-02-01</DATE_PUBLICATION></INDEXATION></GESTION>
    </ann>
    """
    core = extract_core(_el(xml), "eforms", "26-100.xml")
    assert core["id_annonce"] == "ef_26-100"
    assert core["type_document"] == "ContractNotice"
    assert core["sous_type_eforms"] == "16"
    assert core["objet"] == "Objet global"

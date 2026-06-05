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

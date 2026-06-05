from lxml import etree

from src.tasks.boamp.flatten import flatten, localname


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

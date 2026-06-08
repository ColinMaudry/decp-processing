"""Génère le CSV de définitions des colonnes du parquet BOAMP (#178).

Fonctions pures : parsing des XSD legacy et du SDK eForms, résolution des
chemins de colonnes vers des définitions humaines.
"""

import re

from lxml import etree

_PREDICATE = re.compile(r"\[[^\]]*\]")
_XS = "{http://www.w3.org/2001/XMLSchema}"
_WS = re.compile(r"\s+")


def _clean_text(text: str) -> str:
    """Compacte les espaces et retire un tiret/espace de tête décoratif."""
    return _WS.sub(" ", text).strip().lstrip("- ").strip()


def normalize_xpath(xpath: str) -> tuple[str, ...]:
    """Chemin xpathAbsolute eForms -> tuple de noms d'éléments locaux.

    Retire les prédicats ``[...]``, les préfixes de namespace (``cac:``…),
    les pas d'attribut (``@x``) et les ``/*`` racines.
    """
    xpath = _PREDICATE.sub("", xpath)
    segments: list[str] = []
    for step in xpath.split("/"):
        step = step.strip()
        if not step or step == "*" or step.startswith("@"):
            continue
        local = step.split(":")[-1]
        if "@" in local:
            continue
        segments.append(local)
    return tuple(segments)


def build_legacy_index(xsd_paths: list) -> dict[str, str]:
    """Mappe nom d'élément local -> définition FR depuis des XSD.

    Plusieurs textes DISTINCTS pour un même nom sont joints par ' / '.
    """
    texts: dict[str, set[str]] = {}
    for path in xsd_paths:
        tree = etree.parse(str(path))
        for el in tree.iter(f"{_XS}element"):
            name = el.get("name")
            if not name:
                continue
            doc = el.find(f"{_XS}annotation/{_XS}documentation")
            if doc is None or not doc.text:
                continue
            cleaned = _clean_text(doc.text)
            if cleaned:
                texts.setdefault(name, set()).add(cleaned)
    return {name: " / ".join(sorted(values)) for name, values in texts.items()}

"""Helpers lxml pour les avis BOAMP (legacy et eForms)."""

from typing import Literal


def localname(tag) -> str:
    """Nom local d'une balise lxml (sans préfixe de namespace)."""
    if not isinstance(tag, str):  # commentaires, PI…
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def find(elem, name: str):
    """Premier descendant (ou soi-même) dont le nom local == name, sinon None."""
    if localname(elem.tag) == name:
        return elem
    for e in elem.iter():
        if localname(e.tag) == name:
            return e
    return None


def direct_child(elem, name: str):
    """Premier enfant direct dont le nom local == name, sinon None."""
    return next((c for c in elem if localname(c.tag) == name), None)


def children(elem, name: str) -> list:
    """Enfants directs dont le nom local == name."""
    return [c for c in elem if localname(c.tag) == name]


def text(elem) -> str | None:
    """Texte strippé d'un élément, None si absent ou vide."""
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def first_child_localname(elem) -> str | None:
    """Nom local du premier enfant élément, sinon None."""
    for c in elem:
        if isinstance(c.tag, str):
            return localname(c.tag)
    return None


def gestion_text(root, name: str) -> str | None:
    """Texte du premier élément `name` sous GESTION."""
    gestion = find(root, "GESTION")
    if gestion is None:
        return None
    return text(find(gestion, name))


def indexation_choice(root, name: str) -> str | None:
    """Nom local du premier enfant de GESTION/INDEXATION/<name>."""
    gestion = find(root, "GESTION")
    indexation = find(gestion, "INDEXATION") if gestion is not None else None
    elem = find(indexation, name) if indexation is not None else None
    return first_child_localname(elem) if elem is not None else None


def _reference(root):
    gestion = find(root, "GESTION")
    return find(gestion, "REFERENCE") if gestion is not None else None


def idweb(root) -> str | None:
    """GESTION/REFERENCE/IDWEB."""
    ref = _reference(root)
    return text(direct_child(ref, "IDWEB")) if ref is not None else None


def annonce_anterieure(root) -> str | None:
    """GESTION/REFERENCE/ANNONCE_ANTERIEUR/IDWEB."""
    ref = _reference(root)
    anterieure = direct_child(ref, "ANNONCE_ANTERIEUR") if ref is not None else None
    return text(direct_child(anterieure, "IDWEB")) if anterieure is not None else None


def detect_format(root) -> Literal["eforms", "legacy"]:
    """'eforms' si un élément EFORMS est présent, sinon 'legacy'."""
    return "eforms" if find(root, "EFORMS") is not None else "legacy"

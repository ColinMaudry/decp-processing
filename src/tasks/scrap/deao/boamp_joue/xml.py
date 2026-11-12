"""Helpers lxml génériques (avis BOAMP, TED…)."""


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


def child_fr(elem, name: str):
    """Enfant direct `name` en français (languageID == "FRA"), sinon le premier."""
    candidats = children(elem, name)
    for c in candidats:
        if c.get("languageID") == "FRA":
            return c
    return candidats[0] if candidats else None

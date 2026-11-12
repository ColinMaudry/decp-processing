"""Helpers lxml propres aux avis BOAMP (enveloppe DILA GESTION)."""

from typing import Literal

from src.tasks.scrap.deao.boamp_joue.xml import (
    direct_child,
    find,
    first_child_localname,
    text,
)


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

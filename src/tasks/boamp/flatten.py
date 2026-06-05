"""Aplatissement générique des annonces BOAMP/eForms (issue #174).

Aucune I/O : fonctions pures sur des éléments lxml.
"""

from src.config import (
    BOAMP_INCLUDED_NATURES,
    BOAMP_MISE_EN_CONCURRENCE_NATURES,
)


def localname(tag) -> str:
    """Nom local d'une balise lxml (sans préfixe de namespace)."""
    if not isinstance(tag, str):  # commentaires, PI…
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def flatten(
    elem,
    start_path: str = "",
    skip_localnames: frozenset | set = frozenset(),
) -> dict[str, list[str]]:
    """Aplatit un sous-arbre en {chemin_feuille: [valeurs...]}.

    - chemin = noms locaux joints par "_", préfixé par ``start_path`` ;
    - seules les feuilles (sans enfant élément) avec texte non vide sont gardées ;
    - les feuilles répétées accumulent plusieurs valeurs ;
    - les sous-arbres dont le nom local est dans ``skip_localnames`` sont ignorés ;
    - les attributs ne sont pas capturés en v1.
    """
    acc: dict[str, list[str]] = {}

    def walk(e, path: str):
        children = [c for c in e if isinstance(c.tag, str)]
        if not children:
            text = (e.text or "").strip()
            if text:
                acc.setdefault(path, []).append(text)
            return
        for child in children:
            name = localname(child.tag)
            if name in skip_localnames:
                continue
            child_path = f"{path}_{name}" if path else name
            walk(child, child_path)

    # If start_path is empty, use the root element's localname
    if not start_path:
        start_path = localname(elem.tag)

    walk(elem, start_path)
    return acc


def _find(elem, name: str):
    """Premier descendant (ou self) dont le nom local == name, sinon None."""
    if localname(elem.tag) == name:
        return elem
    for e in elem.iter():
        if localname(e.tag) == name:
            return e
    return None


def _first_child_localname(elem) -> str | None:
    """Nom local du premier enfant élément, sinon None."""
    for c in elem:
        if isinstance(c.tag, str):
            return localname(c.tag)
    return None


def detect_format(root) -> str:
    """'eforms' si un élément EFORMS est présent, sinon 'boamp' (legacy v240)."""
    return "eforms" if _find(root, "EFORMS") is not None else "boamp"


def parse_type_avis(root):
    """(nature, sous_nature, statut, famille) depuis GESTION/REFERENCE/TYPE_AVIS.

    Chaque valeur est le nom local de l'enfant (ex. NATURE -> 'APPEL_OFFRE',
    sous-nature = enfant de cet enfant -> 'MISE_EN_CONCURRENCE'), ou None.
    """
    type_avis = _find(root, "TYPE_AVIS")
    if type_avis is None:
        return None, None, None, None
    nature_el = _find(type_avis, "NATURE")
    nature = _first_child_localname(nature_el) if nature_el is not None else None
    sub = None
    if nature_el is not None:
        nature_child = next((c for c in nature_el if isinstance(c.tag, str)), None)
        if nature_child is not None:
            sub = _first_child_localname(nature_child)
    famille_el = _find(type_avis, "FAMILLE")
    famille = _first_child_localname(famille_el) if famille_el is not None else None
    statut_el = _find(type_avis, "STATUT")
    statut = _first_child_localname(statut_el) if statut_el is not None else None
    return nature, sub, statut, famille


def is_included(nature: str | None, sous_nature: str | None) -> bool:
    """Décide si l'avis entre dans le périmètre (liste blanche NATURE)."""
    if nature in BOAMP_INCLUDED_NATURES:
        return True
    if (
        nature in BOAMP_MISE_EN_CONCURRENCE_NATURES
        and sous_nature == "MISE_EN_CONCURRENCE"
    ):
        return True
    return False


def _text(elem) -> str | None:
    """Extrait le texte d'un élément, ou None si vide."""
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def find_lots(root, source_format: str) -> list:
    """Liste des éléments-lot selon le format.

    - eforms: ProcurementProjectLot éléments
    - boamp: lot éléments enfants d'un conteneur lots
    """
    if source_format == "eforms":
        return [e for e in root.iter() if localname(e.tag) == "ProcurementProjectLot"]
    lots = []
    for container in root.iter():
        if localname(container.tag) == "lots":
            lots.extend(c for c in container if localname(c.tag) == "lot")
    return lots


def extract_lot_core(lot, source_format: str) -> dict:
    """{lot_id, lot_intitule} pour un élément-lot.

    - eforms: ID[@schemeName='Lot'] et Name sous ProcurementProject
    - boamp: numLot et description directs
    """
    if source_format == "eforms":
        lot_id = None
        for e in lot.iter():
            if localname(e.tag) == "ID" and e.get("schemeName") == "Lot":
                lot_id = _text(e)
                break
        name_el = next((e for e in lot.iter() if localname(e.tag) == "Name"), None)
        return {"lot_id": lot_id, "lot_intitule": _text(name_el)}
    # legacy boamp
    num = next((c for c in lot if localname(c.tag) == "numLot"), None)
    desc = next((c for c in lot if localname(c.tag) == "description"), None)
    return {"lot_id": _text(num), "lot_intitule": _text(desc)}

"""Aplatissement générique des annonces BOAMP/eForms (issue #174).

Aucune I/O : fonctions pures sur des éléments lxml.
"""


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

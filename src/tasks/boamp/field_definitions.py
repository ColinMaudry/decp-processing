"""Génère le CSV de définitions des colonnes du parquet BOAMP (#178).

Fonctions pures : parsing des XSD legacy et du SDK eForms, résolution des
chemins de colonnes vers des définitions humaines.
"""

import re

_PREDICATE = re.compile(r"\[[^\]]*\]")


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

"""Classement des avis BOAMP : retenu ou non, typeAvis, preInformation."""

from src.config import BOAMP_INCLUDED_NATURES, BOAMP_PRE_INFORMATION_NATURES
from src.tasks.deao.classement import Classement
from src.tasks.deao.sources.boamp.xml import detect_format
from src.tasks.deao.xml import find, first_child_localname


def classify(root) -> Classement:
    type_avis = find(root, "TYPE_AVIS")
    nature_el = find(type_avis, "NATURE") if type_avis is not None else None
    statut_el = find(type_avis, "STATUT") if type_avis is not None else None
    nature = first_child_localname(nature_el) if nature_el is not None else None
    statut = first_child_localname(statut_el) if statut_el is not None else None
    sous = None
    if nature_el is not None:
        nature_child = next((c for c in nature_el if isinstance(c.tag, str)), None)
        if nature_child is not None:
            sous = first_child_localname(nature_child)

    format_source = detect_format(root)
    retenu = nature in BOAMP_INCLUDED_NATURES | BOAMP_PRE_INFORMATION_NATURES
    if not retenu:
        return Classement(nature, False, None, None, format_source)

    if nature == "ANNULATION" or statut == "ANNULATION":
        type_avis_val = "annulation"
    elif nature == "RECTIFICATIF" or statut == "RECTIFICATIF":
        type_avis_val = "rectificatif"
    else:
        type_avis_val = "initial"

    pre_information = None
    if type_avis_val == "initial":
        pre_information = (
            nature in BOAMP_PRE_INFORMATION_NATURES and sous != "MISE_EN_CONCURRENCE"
        )
    return Classement(nature, True, type_avis_val, pre_information, format_source)

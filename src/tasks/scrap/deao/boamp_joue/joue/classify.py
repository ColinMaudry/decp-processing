"""Classement des avis JOUE (eForms lus via TED) : retenu ou non, typeAvis, preInformation."""

from src.tasks.scrap.deao.boamp_joue.classement import Classement
from src.tasks.scrap.deao.boamp_joue.eforms import change_reason, changed_notice_id
from src.tasks.scrap.deao.boamp_joue.xml import find


def liste_nom(notice) -> str | None:
    """Type de formulaire : cbc:NoticeTypeCode/@listName (planning, competition, result…)."""
    code = find(notice, "NoticeTypeCode")
    return code.get("listName") if code is not None else None


def classify_joue(notice) -> Classement:
    nature = liste_nom(notice)
    if nature not in ("planning", "competition"):
        return Classement(nature, False, None, None, "eforms")
    if changed_notice_id(notice):
        type_avis = (
            "annulation" if change_reason(notice) == "cancel" else "rectificatif"
        )
        return Classement(nature, True, type_avis, None, "eforms")
    return Classement(nature, True, "initial", nature == "planning", "eforms")

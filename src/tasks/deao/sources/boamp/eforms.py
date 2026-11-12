"""Enveloppe BOAMP du mapping eForms : repli INDEXATION et urlAvis."""

from collections import Counter

from src.config import BOAMP_AVIS_URL_TEMPLATE
from src.tasks.deao.classement import Classement
from src.tasks.deao.codes import (
    PROCEDURE_INDEXATION,
    TYPE_INDEXATION,
    libelle,
)
from src.tasks.deao.eforms import map_notice
from src.tasks.deao.sources.boamp.xml import idweb, indexation_choice
from src.tasks.deao.values import parse_datetime_paris
from src.tasks.deao.xml import find, text


def _date_limite_indexation(root):
    indexation = find(root, "INDEXATION")
    limite = find(indexation, "DATE_LIMITE_REPONSE") if indexation is not None else None
    return parse_datetime_paris(text(limite))


def map_eforms(root, classement: Classement, compteur: Counter) -> list[dict]:
    """Lignes avis-lot d'un avis BOAMP eForms (sans les champs ajoutés par parse.py)."""
    eforms = find(root, "EFORMS")
    notice = next((c for c in eforms if isinstance(c.tag, str)), None)
    if notice is None:
        return []
    type_marche = libelle(
        TYPE_INDEXATION, indexation_choice(root, "NATURE_MARCHE"), compteur, "type"
    )
    procedure = libelle(
        PROCEDURE_INDEXATION,
        indexation_choice(root, "TYPE_PROCEDURE"),
        compteur,
        "procedure",
    )
    lignes = map_notice(notice, compteur, type_marche=type_marche, procedure=procedure)
    url = BOAMP_AVIS_URL_TEMPLATE.format(idweb=idweb(root))
    repli = None
    resultat = []
    for ligne in lignes:
        if ligne["dateLimiteReponse"] is None:
            repli = repli or _date_limite_indexation(root)
            ligne = {**ligne, "dateLimiteReponse": repli}
        resultat.append({**ligne, "urlAvis": url})
    return resultat

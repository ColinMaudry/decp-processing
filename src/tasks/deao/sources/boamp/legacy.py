"""Mapping des avis BOAMP legacy (schéma 3.2.5) vers des lignes avis-lot."""

import re
from collections import Counter

from src.config import BOAMP_AVIS_URL_TEMPLATE
from src.tasks.deao.classement import Classement
from src.tasks.deao.codes import (
    PROCEDURE_INDEXATION,
    TYPE_INDEXATION,
    libelle,
)
from src.tasks.deao.sources.boamp.xml import idweb, indexation_choice
from src.tasks.deao.values import duree_mois, normalize_lot_id, parse_datetime_paris
from src.tasks.deao.xml import children, direct_child, find, localname, text
from src.tasks.utils import siret_valide

_SEPARATEUR_TEXTE = " | "


def _walk(elem, exclure_lots: bool):
    """Parcourt elem et ses descendants ; ne descend pas dans `lots` si exclure_lots."""
    yield elem
    for child in elem:
        if not isinstance(child.tag, str):
            continue
        if exclure_lots and localname(child.tag) == "lots":
            continue
        yield from _walk(child, exclure_lots)


def _find(scope, name: str, exclure_lots: bool = False):
    if scope is None:
        return None
    return next(
        (e for e in _walk(scope, exclure_lots) if localname(e.tag) == name), None
    )


def _find_text(scope, name: str, exclure_lots: bool = False) -> str | None:
    return text(_find(scope, name, exclure_lots))


def _sans_espaces(valeur: str | None) -> str | None:
    if valeur is None:
        return None
    return re.sub(r"\s+", "", valeur) or None


def _texte_complet(elem) -> str | None:
    """Texte de elem, ou concaténation des textes descendants si structuré."""
    morceaux = [t for e in elem.iter() if (t := text(e))]
    return _SEPARATEUR_TEXTE.join(morceaux) or None


def _float(valeur: str | None) -> float | None:
    if valeur is None:
        return None
    try:
        return float(valeur.replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def _duree(scope, exclure_lots: bool) -> int | None:
    direct = _find_text(scope, "dureeMois", exclure_lots)
    if direct is not None:
        return duree_mois(direct, "mois")
    for conteneur in ("duree", "dureeLot"):
        el = _find(scope, conteneur, exclure_lots)
        if el is None:
            continue
        mois = duree_mois(_find_text(el, "nbMois"), "mois")
        if mois is not None:
            return mois
        jours = duree_mois(_find_text(el, "nbJours"), "jours")
        if jours is not None:
            return jours
    return None


def _montant(scope, exclure_lots: bool) -> float | None:
    for nom in ("valeurEstimee", "estimationValeur"):
        el = _find(scope, nom, exclure_lots)
        if el is not None:
            return _float(text(direct_child(el, "valeur")))
    return None


def _cpv(scope, exclure_lots: bool) -> str | None:
    el = _find(scope, "classPrincipale", exclure_lots)
    return text(el)


def _lieu_cp(scope, exclure_lots: bool) -> str | None:
    for nom in ("lieuExecution", "lieuExecutionLivraison"):
        lieu = _find(scope, nom, exclure_lots)
        if lieu is not None:
            cp = _sans_espaces(_find_text(lieu, "cp"))
            if cp:
                return cp
    return None


def _url_consultation(scope) -> str | None:
    url = _find_text(scope, "urlDocConsul", True)
    if url:
        return url
    complt = _find(scope, "adressesComplt", True)
    document = _find(complt, "document")
    url = _find_text(document, "url") if document is not None else None
    return url or _find_text(scope, "urlProfilAcheteur", True)


def _techniques(scope) -> list[str]:
    noms = {localname(e.tag).lower() for e in _walk(scope, True)}
    techniques = []
    if "accordcadre" in noms:
        techniques.append("Accord-cadre")
    if "sad" in noms:
        techniques.append("Système d'acquisition dynamique")
    return techniques


def _acheteur(organisme) -> dict:
    nom = _find_text(organisme, "nomOfficiel") or _find_text(
        organisme, "acheteurPublic"
    )
    type_id = _find(organisme, "typeIdentificationNational")
    code = _sans_espaces(_find_text(organisme, "codeIdentificationNational"))
    acheteur_id = (
        code
        if type_id is not None
        and direct_child(type_id, "siret") is not None
        and siret_valide(code)
        else None
    )
    return {
        "acheteur_nom": nom,
        "acheteur_id": acheteur_id,
        "acheteur_adresse_voie": _find_text(organisme, "nomvoie"),
        "acheteur_adresse_cp": _sans_espaces(_find_text(organisme, "cp")),
        "acheteur_adresse_ville": _find_text(organisme, "ville"),
        "acheteur_adresse_nuts": None,
    }


def _date_limite(root, notice):
    indexation = find(root, "INDEXATION")
    valeur = _find_text(indexation, "DATE_LIMITE_REPONSE")
    if valeur is None:
        procedure = _find(notice, "procedure", True)
        valeur = _find_text(procedure, "dateReceptionOffres")
    if valeur is None:
        delais = _find(notice, "delais", True)
        valeur = _find_text(delais, "receptionOffres")
    return parse_datetime_paris(valeur)


def _lots(notice) -> list:
    return [
        lot
        for conteneur in _walk(notice, False)
        if localname(conteneur.tag) == "lots"
        for lot in children(conteneur, "lot")
    ]


def _ids_lots(lots: list) -> list[str]:
    numeros = [normalize_lot_id(text(direct_child(lot, "numLot"))) for lot in lots]
    if None in numeros or len(set(numeros)) != len(numeros):
        return [str(rang) for rang in range(1, len(lots) + 1)]
    return numeros


def map_legacy(root, classement: Classement, compteur: Counter) -> list[dict]:
    """Lignes avis-lot d'un avis legacy 3.2.5 (sans les champs ajoutés par parse.py)."""
    donnees = find(root, "DONNEES")
    famille = next((c for c in donnees if isinstance(c.tag, str)), None)
    if famille is None:
        return []
    organisme = _find(famille, "organisme")

    indexation = find(root, "INDEXATION")
    objet = (
        _find_text(indexation, "RESUME_OBJET")
        or _find_text(famille, "intitule", True)
        or _find_text(famille, "objet", True)
    )
    description = None
    for el in _walk(famille, True):
        if localname(el.tag) == "description":
            description = text(el) or text(direct_child(el, "objet"))
            if description:
                break
    infos = _find(famille, "infosRectif", True)
    if infos is None:
        infos = _find(famille, "infosAnnulation", True)

    commun = {
        "referenceAcheteur": _find_text(famille, "numeroReference", True),
        "objet": objet,
        "description": description,
        "type": libelle(
            TYPE_INDEXATION,
            indexation_choice(root, "NATURE_MARCHE"),
            compteur,
            "type",
        ),
        "procedure": libelle(
            PROCEDURE_INDEXATION,
            indexation_choice(root, "TYPE_PROCEDURE"),
            compteur,
            "procedure",
        ),
        "dateLimiteReponse": _date_limite(root, famille),
        "techniques": _techniques(famille),
        "urlConsultation": _url_consultation(famille),
        "urlAvis": BOAMP_AVIS_URL_TEMPLATE.format(idweb=idweb(root)),
        "infosRectificatif": _texte_complet(infos) if infos is not None else None,
        **_acheteur(organisme),
    }
    notice_cpv = _cpv(famille, True)
    notice_duree = _duree(famille, True)
    notice_montant = _montant(famille, True)
    notice_lieu = _lieu_cp(famille, True)

    lots = _lots(famille) if classement.typeAvis == "initial" else []
    if not lots:
        return [
            {
                **commun,
                "lotId": "0",
                "lotIntitule": None,
                "codeCPV": notice_cpv,
                "dureeMois": notice_duree,
                "montantEstime": notice_montant,
                "lieuExecution_code": notice_lieu,
                "lieuExecution_typeCode": "Code postal" if notice_lieu else None,
            }
        ]

    rows = []
    for lot_id, lot in zip(_ids_lots(lots), lots, strict=True):
        duree = _duree(lot, False)
        montant = _montant(lot, False)
        lieu = _lieu_cp(lot, False) or notice_lieu
        rows.append(
            {
                **commun,
                "lotId": lot_id,
                "lotIntitule": _find_text(lot, "intitule")
                or _find_text(lot, "description"),
                "codeCPV": _cpv(lot, False) or notice_cpv,
                "dureeMois": duree if duree is not None else notice_duree,
                "montantEstime": montant if montant is not None else notice_montant,
                "lieuExecution_code": lieu,
                "lieuExecution_typeCode": "Code postal" if lieu else None,
            }
        )
    return rows

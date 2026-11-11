"""Mapping des avis BOAMP eForms (UBL ContractNotice / PriorInformationNotice)."""

import re
from collections import Counter

from src.config import BOAMP_AVIS_URL_TEMPLATE
from src.tasks.deao.sources.boamp.classify import Classement
from src.tasks.deao.sources.boamp.codes import (
    PROCEDURE_EFORMS,
    PROCEDURE_INDEXATION,
    TYPE_EFORMS,
    TYPE_INDEXATION,
    libelle,
)
from src.tasks.deao.sources.boamp.xml import (
    children,
    direct_child,
    find,
    idweb,
    indexation_choice,
    localname,
    text,
)
from src.tasks.deao.values import (
    departement_depuis_nuts,
    duree_mois,
    normalize_lot_id,
    parse_datetime_paris,
)
from src.tasks.utils import siret_valide

_SEPARATEUR_TEXTE = " | "
_SCHEMES_LOT = {"Lot", "Part"}


def _sans_espaces(valeur: str | None) -> str | None:
    if valeur is None:
        return None
    return re.sub(r"\s+", "", valeur) or None


def _chemin(elem, *noms: str):
    """Descend par enfants directs successifs ; None si un maillon manque."""
    for nom in noms:
        if elem is None:
            return None
        elem = direct_child(elem, nom)
    return elem


def _float(valeur: str | None) -> float | None:
    if valeur is None:
        return None
    try:
        return float(valeur.replace(",", ".").replace(" ", ""))
    except ValueError:
        return None


def _buyer_org_id(notice) -> str | None:
    """ID de l'organisation acheteuse : ContractingParty/Party/PartyIdentification/ID
    (enfants directs, pas le ServiceProviderParty imbriqué)."""
    contracting = direct_child(notice, "ContractingParty")
    return text(_chemin(contracting, "Party", "PartyIdentification", "ID"))


def _find_company_by_id(root, org_id: str):
    """Company dont PartyIdentification/ID == org_id."""
    for org in root.iter():
        if localname(org.tag) != "Organization":
            continue
        company = direct_child(org, "Company")
        if text(_chemin(company, "PartyIdentification", "ID")) == org_id:
            return company
    return None


def _acheteur(root, notice) -> dict:
    org_id = _buyer_org_id(notice)
    company = _find_company_by_id(root, org_id) if org_id else None
    adresse = direct_child(company, "PostalAddress") if company is not None else None
    siret = _sans_espaces(
        text(_chemin(company, "PartyLegalEntity", "CompanyID"))
        if company is not None
        else None
    )
    return {
        "acheteur_nom": text(_chemin(company, "PartyName", "Name")),
        "acheteur_id": siret if siret_valide(siret) else None,
        "acheteur_adresse_voie": text(_chemin(adresse, "StreetName")),
        "acheteur_adresse_cp": _sans_espaces(text(_chemin(adresse, "PostalZone"))),
        "acheteur_adresse_ville": text(_chemin(adresse, "CityName")),
        "acheteur_adresse_nuts": text(_chemin(adresse, "CountrySubentityCode")),
    }


def _cpv(projet) -> str | None:
    return text(
        _chemin(projet, "MainCommodityClassification", "ItemClassificationCode")
    )


def _duree(projet) -> int | None:
    periode = direct_child(projet, "PlannedPeriod") if projet is not None else None
    mesure = find(periode, "DurationMeasure") if periode is not None else None
    if mesure is None:
        return None
    return duree_mois(text(mesure), mesure.get("unitCode", ""))


def _montant(projet) -> float | None:
    if projet is None:
        return None
    return _float(text(find(projet, "EstimatedOverallContractAmount")))


def _nuts_lieu(projet) -> str | None:
    lieu = direct_child(projet, "RealizedLocation") if projet is not None else None
    return text(find(lieu, "CountrySubentityCode")) if lieu is not None else None


def _description(projet) -> str | None:
    return text(direct_child(projet, "Description")) if projet is not None else None


def _url_consultation(lot) -> str | None:
    termes = direct_child(lot, "TenderingTerms")
    reference = (
        direct_child(termes, "CallForTendersDocumentReference")
        if termes is not None
        else None
    )
    uri = find(reference, "URI") if reference is not None else None
    url = text(uri)
    # certains éditeurs encodent deux fois les "&"
    return url.replace("&amp;", "&") if url else None


def _techniques(lot) -> list[str]:
    techniques = []
    processus = direct_child(lot, "TenderingProcess")
    for systeme in (
        children(processus, "ContractingSystem") if processus is not None else []
    ):
        code = text(direct_child(systeme, "ContractingSystemTypeCode")) or ""
        if code.startswith("fa-") and "Accord-cadre" not in techniques:
            techniques.append("Accord-cadre")
        elif code.startswith("dps-") and (
            "Système d'acquisition dynamique" not in techniques
        ):
            techniques.append("Système d'acquisition dynamique")
    return techniques


def _date_limite(lot, root):
    periode = _chemin(lot, "TenderingProcess", "TenderSubmissionDeadlinePeriod")
    date = text(direct_child(periode, "EndDate")) if periode is not None else None
    if date:
        return parse_datetime_paris(date, text(direct_child(periode, "EndTime")))
    indexation = find(root, "INDEXATION")
    limite = find(indexation, "DATE_LIMITE_REPONSE") if indexation is not None else None
    return parse_datetime_paris(text(limite))


def _id_lot(lot) -> str | None:
    for ident in children(lot, "ID"):
        if ident.get("schemeName") in _SCHEMES_LOT:
            return normalize_lot_id(text(ident))
    return None


def _infos_rectificatif(root) -> str | None:
    textes = [
        t
        for e in root.iter()
        if localname(e.tag) == "ChangeDescription"
        if (t := text(e))
    ]
    return _SEPARATEUR_TEXTE.join(textes) or None


def map_eforms(root, classement: Classement, compteur: Counter) -> list[dict]:
    """Lignes avis-lot d'un avis eForms (sans les champs ajoutés par parse.py)."""
    eforms = find(root, "EFORMS")
    notice = next((c for c in eforms if isinstance(c.tag, str)), None)
    if notice is None:
        return []
    pp = direct_child(notice, "ProcurementProject")

    type_marche = libelle(
        TYPE_INDEXATION, indexation_choice(root, "NATURE_MARCHE"), compteur, "type"
    ) or libelle(
        TYPE_EFORMS,
        text(direct_child(pp, "ProcurementTypeCode")) if pp is not None else None,
        compteur,
        "type",
    )
    code_procedure = (
        text(direct_child(direct_child(notice, "TenderingProcess"), "ProcedureCode"))
        if direct_child(notice, "TenderingProcess") is not None
        else None
    )
    procedure = libelle(
        PROCEDURE_INDEXATION,
        indexation_choice(root, "TYPE_PROCEDURE"),
        compteur,
        "procedure",
    ) or libelle(PROCEDURE_EFORMS, code_procedure, compteur, "procedure")

    commun = {
        "referenceAcheteur": text(direct_child(pp, "ID")) if pp is not None else None,
        "objet": text(direct_child(pp, "Name")) if pp is not None else None,
        "type": type_marche,
        "procedure": procedure,
        "urlAvis": BOAMP_AVIS_URL_TEMPLATE.format(idweb=idweb(root)),
        "infosRectificatif": _infos_rectificatif(root),
        **_acheteur(root, notice),
    }

    lots = children(notice, "ProcurementProjectLot")
    if not lots:
        lieu = departement_depuis_nuts(_nuts_lieu(pp))
        return [
            {
                **commun,
                "lotId": "0",
                "lotIntitule": None,
                "description": _description(pp),
                "codeCPV": _cpv(pp),
                "dureeMois": _duree(pp),
                "montantEstime": _montant(pp),
                "dateLimiteReponse": _date_limite(None, root),
                "techniques": [],
                "urlConsultation": None,
                "lieuExecution_code": lieu,
                "lieuExecution_typeCode": "Code département" if lieu else None,
            }
        ]

    rows = []
    for rang, lot in enumerate(lots, start=1):
        lpp = direct_child(lot, "ProcurementProject")
        duree = _duree(lpp)
        montant = _montant(lpp)
        lieu = departement_depuis_nuts(_nuts_lieu(lpp)) or departement_depuis_nuts(
            _nuts_lieu(pp)
        )
        rows.append(
            {
                **commun,
                "lotId": _id_lot(lot) or str(rang),
                "lotIntitule": text(direct_child(lpp, "Name"))
                if lpp is not None
                else None,
                "description": _description(lpp) or _description(pp),
                "codeCPV": _cpv(lpp) or _cpv(pp),
                "dureeMois": duree if duree is not None else _duree(pp),
                "montantEstime": montant if montant is not None else _montant(pp),
                "dateLimiteReponse": _date_limite(lot, root),
                "techniques": _techniques(lot),
                "urlConsultation": _url_consultation(lot),
                "lieuExecution_code": lieu,
                "lieuExecution_typeCode": "Code département" if lieu else None,
            }
        )
    return rows

"""Mapping eForms (UBL ContractNotice / PriorInformationNotice…), commun aux sources."""

import re
from collections import Counter

from src.tasks.deao.values import (
    departement_depuis_nuts,
    duree_mois,
    normalize_lot_id,
    parse_datetime_paris,
    unite_duree_connue,
)
from src.tasks.scrap.deao.boamp_joue.codes import PROCEDURE_EFORMS, TYPE_EFORMS, libelle
from src.tasks.scrap.deao.boamp_joue.xml import (
    child_fr,
    children,
    direct_child,
    find,
    localname,
    text,
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
    nom = _chemin(company, "PartyName")
    return {
        "acheteur_nom": text(child_fr(nom, "Name")) if nom is not None else None,
        "acheteur_id": siret if siret_valide(siret) else None,
        "acheteur_adresse_voie": text(_chemin(adresse, "StreetName")),
        "acheteur_adresse_cp": _sans_espaces(text(_chemin(adresse, "PostalZone"))),
        "acheteur_adresse_ville": text(_chemin(adresse, "CityName")),
        "acheteur_adresse_nuts": text(_chemin(adresse, "CountrySubentityCode")),
        "acheteur_pays": text(_chemin(adresse, "Country", "IdentificationCode")),
    }


def _cpv(projet) -> str | None:
    return text(
        _chemin(projet, "MainCommodityClassification", "ItemClassificationCode")
    )


def _duree(projet, compteur: Counter) -> int | None:
    periode = direct_child(projet, "PlannedPeriod") if projet is not None else None
    mesure = find(periode, "DurationMeasure") if periode is not None else None
    if mesure is None:
        return None
    unite = mesure.get("unitCode", "")
    if not unite_duree_connue(unite):
        compteur[f"non_mappe:unite_duree:{unite}"] += 1
        return None
    return duree_mois(text(mesure), unite)


def _montant(projet) -> float | None:
    if projet is None:
        return None
    return _float(text(find(projet, "EstimatedOverallContractAmount")))


def _nuts_lieu(projet) -> str | None:
    lieu = direct_child(projet, "RealizedLocation") if projet is not None else None
    return text(find(lieu, "CountrySubentityCode")) if lieu is not None else None


def _description(projet) -> str | None:
    return text(child_fr(projet, "Description")) if projet is not None else None


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


def _date_limite(lot):
    periode = _chemin(lot, "TenderingProcess", "TenderSubmissionDeadlinePeriod")
    date = text(direct_child(periode, "EndDate")) if periode is not None else None
    if date:
        return parse_datetime_paris(date, text(direct_child(periode, "EndTime")))
    return None


def _id_lot(lot) -> str | None:
    for ident in children(lot, "ID"):
        if ident.get("schemeName") in _SCHEMES_LOT:
            return normalize_lot_id(text(ident))
    return None


def _ids_lots(lots: list) -> list[str]:
    """IDs normalisés des lots ; le rang pour tous si un ID manque ou se répète."""
    ids = [_id_lot(lot) for lot in lots]
    if None in ids or len(set(ids)) != len(ids):
        return [str(rang) for rang in range(1, len(lots) + 1)]
    return ids


def _infos_rectificatif(notice) -> str | None:
    textes = [
        t
        for e in notice.iter()
        if isinstance(e.tag, str)
        if direct_child(e, "ChangeDescription") is not None
        if (t := text(child_fr(e, "ChangeDescription")))
    ]
    return _SEPARATEUR_TEXTE.join(textes) or None


def notice_id(notice) -> str | None:
    """cbc:ID[@schemeName="notice-id"] (enfant direct), en minuscules."""
    for ident in children(notice, "ID"):
        if ident.get("schemeName") == "notice-id":
            valeur = text(ident)
            return valeur.lower() if valeur else None
    return None


def changed_notice_id(notice) -> str | None:
    """Avis modifié (efbc:ChangedNoticeIdentifier), sans le suffixe de version -NN."""
    valeur = text(find(notice, "ChangedNoticeIdentifier"))
    if valeur is None:
        return None
    return re.sub(
        r"^([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})-\d{2}$",
        r"\1",
        valeur,
        flags=re.IGNORECASE,
    ).lower()


def change_reason(notice) -> str | None:
    """cbc:ReasonCode[@listName="change-corrig-justification"]."""
    for e in notice.iter():
        if (
            localname(e.tag) == "ReasonCode"
            and e.get("listName") == "change-corrig-justification"
        ):
            return text(e)
    return None


def map_notice(
    notice,
    compteur: Counter,
    *,
    type_marche: str | None = None,
    procedure: str | None = None,
) -> list[dict]:
    """Lignes avis-lot d'un avis eForms (sans urlAvis ni champs de la source).

    `type_marche` / `procedure` non nuls l'emportent sur les codes eForms.
    """
    pp = direct_child(notice, "ProcurementProject")

    type_marche = type_marche or libelle(
        TYPE_EFORMS,
        text(direct_child(pp, "ProcurementTypeCode")) if pp is not None else None,
        compteur,
        "type",
    )
    process = direct_child(notice, "TenderingProcess")
    code_procedure = (
        text(direct_child(process, "ProcedureCode")) if process is not None else None
    )
    procedure = procedure or libelle(
        PROCEDURE_EFORMS, code_procedure, compteur, "procedure"
    )

    commun = {
        "referenceAcheteur": text(direct_child(pp, "ID")) if pp is not None else None,
        "objet": text(child_fr(pp, "Name")) if pp is not None else None,
        "type": type_marche,
        "procedure": procedure,
        "infosRectificatif": _infos_rectificatif(notice),
        **_acheteur(notice, notice),
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
                "dureeMois": _duree(pp, compteur),
                "montantEstime": _montant(pp),
                "dateLimiteReponse": _date_limite(None),
                "techniques": [],
                "urlConsultation": None,
                "lieuExecution_code": lieu,
                "lieuExecution_typeCode": "Code département" if lieu else None,
            }
        ]

    rows = []
    for lot, lot_id in zip(lots, _ids_lots(lots), strict=True):
        lpp = direct_child(lot, "ProcurementProject")
        duree = _duree(lpp, compteur)
        montant = _montant(lpp)
        lieu = departement_depuis_nuts(_nuts_lieu(lpp)) or departement_depuis_nuts(
            _nuts_lieu(pp)
        )
        rows.append(
            {
                **commun,
                "lotId": lot_id,
                "lotIntitule": text(child_fr(lpp, "Name")) if lpp is not None else None,
                "description": _description(lpp) or _description(pp),
                "codeCPV": _cpv(lpp) or _cpv(pp),
                "dureeMois": duree if duree is not None else _duree(pp, compteur),
                "montantEstime": montant if montant is not None else _montant(pp),
                "dateLimiteReponse": _date_limite(lot),
                "techniques": _techniques(lot),
                "urlConsultation": _url_consultation(lot),
                "lieuExecution_code": lieu,
                "lieuExecution_typeCode": "Code département" if lieu else None,
            }
        )
    return rows

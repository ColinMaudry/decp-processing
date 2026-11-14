"""Lignes au schéma publié (une par consultation et par lot) pour www.marches-publics.info (AWS)."""

from collections import Counter

from src.tasks.scrap.deao.atexo.lignes import AUTRE_PROCEDURE, PROCEDURES
from src.tasks.scrap.deao.aws.parse import Avis, AvisListe, LotAvis

# Valeurs du champ « Mode » des pages d'avis, ramenées aux libellés d'Atexo/BOAMP
MODES: dict[str, str] = {
    "procédure adaptée ouverte": PROCEDURES["MAPA"],
    "procédure adaptée restreinte": PROCEDURES["MAPA"],
    "procédure adaptée": PROCEDURES["MAPA"],
    "procédure ouverte": PROCEDURES["AOO"],
    "appel d'offres ouvert": PROCEDURES["AOO"],
    "procédure restreinte": PROCEDURES["AOR"],
    "appel d'offres restreint": PROCEDURES["AOR"],
    "procédure négociée": PROCEDURES["PN"],
    "procédure avec négociation": PROCEDURES["PN"],
    "dialogue compétitif": PROCEDURES["DC"],
    "concession": PROCEDURES["CONC"],
    # Libellés BOAMP des concours (TYPE_PROCEDURE CONCOURS_OUVERT / CONCOURS_RESTREINT)
    "concours ouvert": "Concours ouvert",
    "concours restreint": "Concours restreint",
}

TECHNIQUES: dict[str, str] = {
    "accord-cadre": "Accord-cadre",
    "concours": "Concours",
    "système de qualification": "Système de qualification",
    "système d'acquisition dynamique": "Système d'acquisition dynamique",
    "catalogue électronique": "Catalogue électronique",
    "enchère électronique": "Enchère électronique",
}
# Technique explicitement absente : aucune technique, sans compteur d'inconnue.
_SANS_TECHNIQUE = {"sans objet"}


def libelle_procedure_aws(mode: str | None, stats: Counter | None = None) -> str:
    """Libellé de procédure BOAMP pour le « Mode » d'un avis ; inconnu : « Autre procédure » et compteur."""
    cle = " ".join((mode or "").lower().split())
    libelle = MODES.get(cle)
    if libelle is not None:
        return libelle
    if stats is not None and mode:
        stats[f"procedure_inconnue:{mode}"] += 1
    return AUTRE_PROCEDURE


def departement(code_postal: str | None) -> str | None:
    """Code département d'un code postal (« 25400 » -> « 25 », « 97411 » -> « 974 », Corse -> 2A/2B)."""
    if not code_postal or len(code_postal) != 5 or not code_postal.isdigit():
        return None
    if code_postal[:2] in ("97", "98"):
        return code_postal[:3]
    if code_postal[:2] == "20":
        return "2A" if code_postal[:3] == "200" or code_postal[:3] == "201" else "2B"
    return code_postal[:2]


def _combine(liste: bool, avis: bool | None) -> bool | None:
    """Liste ou avis vrai -> vrai ; avis faux explicite -> faux ; sinon (liste muette) -> nul."""
    if liste or avis:
        return True
    return False if avis is False else None


def _techniques(technique: str | None, stats: Counter | None) -> list[str]:
    if not technique:
        return []
    cle = " ".join(technique.lower().split())
    if cle in _SANS_TECHNIQUE:
        return []
    libelle = TECHNIQUES.get(cle)
    if libelle is None:
        if stats is not None:
            stats[f"technique_inconnue:{technique}"] += 1
        return []
    return [libelle]


def lignes_avis(
    code: str,
    url_base: str,
    liste: AvisListe,
    avis: Avis,
    url_rc: str,
    stats: Counter | None = None,
) -> list[dict]:
    """Une ligne par lot, ou une ligne `lotId` = '0' pour un avis non alloti."""
    lieu_acheteur = departement(avis.adresse_cp) or departement(liste.code_postal)
    commun = {
        "id": liste.idm,
        "idDernierAvis": liste.numero_avis,
        "referenceAcheteur": avis.reference or liste.reference,
        "acheteur_id": avis.siret,
        "acheteur_nom": avis.acheteur_nom or liste.acheteur_nom,
        "acheteur_adresse_voie": avis.adresse_voie,
        "acheteur_adresse_cp": avis.adresse_cp or liste.code_postal,
        "acheteur_adresse_ville": avis.adresse_ville,
        "acheteur_pays": "FRA",
        "objet": avis.objet or liste.objet,
        "description": avis.description,
        "procedure": libelle_procedure_aws(avis.mode, stats),
        "techniques": _techniques(avis.technique, stats),
        "dureeMois": avis.duree_mois,
        "datePublicationInitiale": avis.date_envoi or liste.date_publication,
        "dateLimiteReponse": avis.date_limite or liste.date_limite,
        "urlConsultation": url_base + liste.chemin_avis,
        "urlAvis": url_base + liste.chemin_avis,
        "urlReglementConsultation": url_rc,
        "urlDce": None,
        "considerationsSociales": _combine(
            liste.considerations_sociales, avis.considerations_sociales
        ),
        "considerationsEnvironnementales": _combine(
            liste.considerations_environnementales,
            avis.considerations_environnementales,
        ),
        "marcheReserve": avis.marche_reserve,
        "variantes": avis.variantes,
        "groupementCommandes": avis.groupement,
        "rectifie": False,
        "infosRectificatifs": [],
        "avisLies": [],
        "sourceDataset": code,
        "sourceFile": None,
    }
    type_marche = avis.type_marche or liste.nature

    def ligne(lot: LotAvis | None, numero: str) -> dict:
        lieu = (departement(lot.code_postal_lieu) if lot else None) or lieu_acheteur
        return {
            **commun,
            "uid": f"{code}_{liste.idm}_{numero}",
            "lotId": numero,
            "lotIntitule": lot.libelle if lot else None,
            "type": type_marche,
            "codeCPV": (lot.code_cpv if lot else None) or avis.cpv_principal,
            "montantEstime": lot.montant_estime if lot else None,
            "lieuExecution_code": lieu,
            "lieuExecution_typeCode": "Code département" if lieu else None,
        }

    if not avis.lots:
        return [ligne(None, "0")]
    return [ligne(lot, lot.numero) for lot in avis.lots]

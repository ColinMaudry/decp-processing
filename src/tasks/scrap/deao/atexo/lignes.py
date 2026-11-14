"""Lignes au schéma publié (une par consultation et par lot) pour les plateformes Atexo."""

from base64 import b64encode
from collections import Counter

import polars as pl

from src.tasks.scrap.deao.atexo.parse import (
    ConsultationListe,
    DetailConsultation,
    Lot,
    libelle_entite_achat,
)

AUTRE_PROCEDURE = "Autre procédure"

PROCEDURES: dict[str, str] = {
    "MAPA": "Procédure adaptée",
    "AOO": "Appel d'offres ouvert",
    "AOR": "Appel d'offres restreint",
    "PAN": "Procédure négociée",
    "PN": "Procédure négociée",
    "PCN": "Procédure négociée",
    "PNSM": "Procédure négociée",
    "DC": "Dialogue compétitif",
    "CONC": "Concession",
}

# Codes observés sur les plateformes sans équivalent propre (« Autre procédure »), non comptés comme inconnus.
_CODES_AUTRES = {"AUT", "SAD-A", "CRES"}


def libelle_procedure(code: str, stats: Counter | None = None) -> str:
    libelle = PROCEDURES.get(code)
    if libelle is not None:
        return libelle
    if stats is not None and code not in _CODES_AUTRES:
        stats[f"procedure_inconnue:{code}"] += 1
    return AUTRE_PROCEDURE


def lignes_consultation(
    code: str,
    url_base: str,
    c: ConsultationListe,
    detail: DetailConsultation | None,
    lots: list[Lot],
    url_dce: str | None,
    stats: Counter | None = None,
) -> list[dict]:
    """Une ligne par lot, ou une ligne `lotId` = '0' pour une consultation non allotie."""
    ref_b64 = b64encode(c.ref_cons.encode()).decode()
    cpv_detail = detail.code_cpv if detail else None
    lieu = c.lieux_execution[0] if c.lieux_execution else None
    commun = {
        "id": c.ref_cons,
        "idDernierAvis": None,
        "referenceAcheteur": c.reference,
        "acheteur_nom": (
            libelle_entite_achat(detail.entite_achat)
            if detail and detail.entite_achat
            else c.organisme
        ),
        "acheteur_adresse_cp": c.code_postal,
        "acheteur_adresse_ville": c.ville,
        "acheteur_pays": "FRA",
        "objet": c.intitule,
        "description": c.objet,
        "procedure": libelle_procedure(c.procedure_code, stats),
        "datePublicationInitiale": c.date_mise_en_ligne,
        "dateLimiteReponse": c.date_limite,
        "lieuExecution_code": lieu,
        "lieuExecution_typeCode": "Code département" if lieu else None,
        "urlConsultation": f"{url_base}/entreprise/consultation/{c.ref_cons}?orgAcronyme={c.org_cons}",
        "urlReglementConsultation": (
            f"{url_base}/index.php?page=Entreprise.EntrepriseDownloadReglement"
            f"&id={ref_b64}&orgAcronyme={c.org_cons}"
        ),
        "urlDce": url_dce,
        "rectifie": False,
        "infosRectificatifs": [],
        "avisLies": [],
        "sourceDataset": code,
        "sourceFile": None,
    }
    if not lots:
        return [
            {
                **commun,
                "uid": f"{code}_{c.ref_cons}_0",
                "lotId": "0",
                "lotIntitule": None,
                "type": c.categorie,
                "codeCPV": cpv_detail,
            }
        ]
    return [
        {
            **commun,
            "uid": f"{code}_{c.ref_cons}_{lot.numero}",
            "lotId": lot.numero,
            "lotIntitule": lot.intitule,
            "type": lot.categorie or c.categorie,
            "codeCPV": lot.code_cpv or cpv_detail,
        }
        for lot in lots
    ]


def lots_depuis_precedent(precedent: pl.DataFrame, ref_cons: str) -> list[Lot]:
    """Lots d'une consultation reconstitués depuis les lignes précédentes (non relus)."""
    lignes = precedent.filter((pl.col("id") == ref_cons) & (pl.col("lotId") != "0"))
    lots = [
        Lot(
            numero=r["lotId"],
            intitule=r["lotIntitule"],
            categorie=r["type"],
            description=r["description"],
            code_cpv=r["codeCPV"],
        )
        for r in lignes.iter_rows(named=True)
    ]
    return sorted(
        lots,
        key=lambda lot: (
            (0, int(lot.numero), "") if lot.numero.isdigit() else (1, 0, lot.numero)
        ),
    )

"""Parsing d'un avis JOUE (XML eForms de TED) et d'un jour d'avis en lignes avis-lot."""

from collections import Counter
from datetime import date

import polars as pl
from lxml import etree

from src.config import JOUE_AVIS_URL_TEMPLATE, TED_NOTICE_XML_URL
from src.tasks.deao.schema import AVIS_LOT_SCHEMA, conform_avis, empty_avis_frame
from src.tasks.scrap.deao.boamp_joue.eforms import (
    changed_notice_id,
    map_notice,
    notice_id,
)
from src.tasks.scrap.deao.boamp_joue.joue.classify import classify_joue
from src.tasks.scrap.deao.boamp_joue.xml import localname, text
from src.tasks.utils import get_logger


def _numero_publication(notice) -> str | None:
    """efbc:NoticePublicationID[@schemeName="ojs-notice-id"] sans zéros de tête."""
    for e in notice.iter():
        if (
            localname(e.tag) == "NoticePublicationID"
            and e.get("schemeName") == "ojs-notice-id"
        ):
            valeur = text(e)
            return valeur.lstrip("0") if valeur else None
    return None


def _date_publication(notice) -> date | None:
    for e in notice.iter():
        if localname(e.tag) == "PublicationDate":
            try:
                return date.fromisoformat((text(e) or "")[:10])
            except ValueError:
                return None
    return None


def parse_avis(xml: bytes, filename: str, compteur: Counter) -> list[dict]:
    """Lignes avis-lot d'un avis ; [] si XML illisible ou avis exclu."""
    compteur["lu"] += 1
    try:
        notice = etree.fromstring(xml)
    except etree.XMLSyntaxError as e:
        compteur["erreur_xml"] += 1
        get_logger("WARNING").warning(f"XML illisible ({filename}) : {e}")
        return []

    classement = classify_joue(notice)
    if not classement.retenu:
        compteur[f"exclu:{classement.nature or 'sans_type'}"] += 1
        return []

    try:
        lignes = map_notice(notice, compteur)
    except Exception as e:
        compteur["erreur_mapping"] += 1
        get_logger("WARNING").warning(
            f"Avis non mappé ({filename}) : {type(e).__name__} : {e}"
        )
        return []

    date_publication = _date_publication(notice)
    if date_publication is None:
        compteur["sans_date_publication"] += 1
    id_avis = _numero_publication(notice) or filename.removesuffix(".xml")
    communs = {
        "sourceDataset": "joue",
        "idAvis": id_avis,
        "avisPrecedent": None,
        "typeAvis": classement.typeAvis,
        "preInformation": classement.preInformation,
        "avisComplet": True,
        "datePublicationAvis": date_publication,
        "formatSource": "eforms",
        "idEforms": notice_id(notice),
        "idEformsPrecedent": changed_notice_id(notice),
        "urlAvis": JOUE_AVIS_URL_TEMPLATE.format(publication_number=id_avis),
        "sourceFile": TED_NOTICE_XML_URL.format(publication_number=id_avis),
    }
    compteur["retenu"] += 1
    return [{**ligne, **communs} for ligne in lignes]


def parse_day(files: dict[str, bytes]) -> tuple[pl.DataFrame, Counter]:
    """DataFrame conforme à AVIS_LOT_SCHEMA pour les fichiers {nom: xml} d'un jour."""
    compteur: Counter = Counter()
    lignes: list[dict] = []
    for nom in sorted(files):
        lignes.extend(parse_avis(files[nom], nom, compteur))
    if not lignes:
        return empty_avis_frame(), compteur
    df = pl.DataFrame(lignes, schema=AVIS_LOT_SCHEMA)
    return conform_avis(df), compteur

"""Parsing d'un avis BOAMP et d'un jour d'avis en lignes avis-lot."""

from collections import Counter
from datetime import date

import polars as pl
from lxml import etree

from src.config import BOAMP_BASE_URL
from src.tasks.deao.schema import AVIS_LOT_SCHEMA, conform_avis, empty_avis_frame
from src.tasks.deao.sources.boamp.classify import classify
from src.tasks.deao.sources.boamp.eforms import map_eforms
from src.tasks.deao.sources.boamp.legacy import map_legacy
from src.tasks.deao.sources.boamp.xml import (
    annonce_anterieure,
    find,
    idweb,
)
from src.tasks.utils import get_logger


def _date_publication(root) -> date | None:
    gestion = find(root, "GESTION")
    indexation = find(gestion, "INDEXATION") if gestion is not None else None
    elem = find(indexation, "DATE_PUBLICATION") if indexation is not None else None
    valeur = (elem.text or "").strip() if elem is not None else ""
    try:
        return date.fromisoformat(valeur[:10])
    except ValueError:
        return None


def parse_avis(xml: bytes, filename: str, compteur: Counter) -> list[dict]:
    """Lignes avis-lot d'un avis ; [] si XML illisible ou avis exclu."""
    compteur["lu"] += 1
    try:
        root = etree.fromstring(xml)
    except etree.XMLSyntaxError as e:
        compteur["erreur_xml"] += 1
        get_logger("WARNING").warning(f"XML illisible ({filename}) : {e}")
        return []

    classement = classify(root)
    if not classement.retenu:
        compteur[f"exclu:{classement.nature}"] += 1
        return []

    if classement.formatSource == "eforms":
        lignes = map_eforms(root, classement, compteur)
    else:
        lignes = map_legacy(root, classement, compteur)

    date_publication = _date_publication(root)
    source_file = (
        f"{BOAMP_BASE_URL}/{date_publication:%Y}/{date_publication:%m}"
        f"/{date_publication:%d}/{filename}"
        if date_publication
        else None
    )
    communs = {
        "sourceDataset": "boamp",
        "idAvis": idweb(root),
        "avisPrecedent": annonce_anterieure(root),
        "typeAvis": classement.typeAvis,
        "preInformation": classement.preInformation,
        "avisComplet": classement.formatSource == "eforms"
        or classement.typeAvis == "initial",
        "datePublicationAvis": date_publication,
        "formatSource": classement.formatSource,
        "sourceFile": source_file,
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

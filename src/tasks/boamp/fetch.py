"""Acquisition des annonces BOAMP depuis l'archive DILA (issue #174)."""

import re
from datetime import date, timedelta

from src.config import (
    BOAMP_ARCHIVE_CUTOFF_YEAR,
    BOAMP_BASE_URL,
)
from src.tasks.get import stream_get

_XML_HREF_RE = re.compile(r'href="(\d{2}-\d+\.xml)"')


def day_index_url(d: date) -> str:
    """URL de l'index HTML d'un jour, routée flux courant / FluxHistorique."""
    ymd = f"{d.year}/{d.month:02d}/{d.day:02d}/"
    if d.year < BOAMP_ARCHIVE_CUTOFF_YEAR:
        return f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/{ymd}"
    return f"{BOAMP_BASE_URL}/{ymd}"


def iter_days(start: date, end: date):
    """Itérateur inclusif des jours d'une plage [start, end]."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def list_day_files(d: date) -> list[str]:
    """Noms des fichiers .xml listés dans l'index d'un jour (triés, dédupliqués)."""
    html = b"".join(stream_get(day_index_url(d))).decode("utf-8", "ignore")
    return sorted(set(_XML_HREF_RE.findall(html)))


def download_xml(d: date, filename: str) -> bytes:
    """Télécharge un fichier d'annonce d'un jour donné."""
    return b"".join(stream_get(day_index_url(d) + filename))

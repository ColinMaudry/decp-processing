"""Acquisition des annonces BOAMP depuis l'archive DILA (issue #174)."""

import re
import tarfile
import tempfile
from datetime import date, timedelta

from src.config import (
    BOAMP_ARCHIVE_CUTOFF_YEAR,
    BOAMP_BACKFILL_ARCHIVES,
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


def year_archive_urls(year: int) -> list[str]:
    """Retourne les URLs des archives .tar.gz pour une année donnée."""
    base = f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/{year}"
    return [f"{base}/{name}.tar.gz" for name in BOAMP_BACKFILL_ARCHIVES]


def iter_archive_xml(year: int):
    """Yield (member_name, xml_bytes) pour chaque .xml des archives .tar.gz d'une année."""
    for url in year_archive_urls(year):
        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
            for chunk in stream_get(url):
                tmp.write(chunk)
            tmp.flush()
            try:
                archive = tarfile.open(tmp.name, "r:gz")
            except (tarfile.ReadError, FileNotFoundError, EOFError):
                continue
            with archive:
                for member in archive:
                    if not member.isfile() or not member.name.endswith(".xml"):
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is not None:
                        yield member.name, extracted.read()

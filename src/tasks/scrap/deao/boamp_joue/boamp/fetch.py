"""Accès réseau à l'index et aux XML de l'archive DILA (issue #178)."""

import re
from datetime import date

import httpx

from src.config import BOAMP_BASE_URL, HTTP_CLIENT, HTTP_HEADERS

_XML_HREF_RE = re.compile(r'href="(\d{2}-\d+\.xml)"')


def day_index_urls(d: date) -> list[str]:
    """URLs d'index d'un jour : flux courant, puis repli FluxHistorique."""
    return [
        f"{BOAMP_BASE_URL}/{d:%Y/%m/%d}/",
        f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/{d:%Y/%m/%d}/",
    ]


def http_get(url: str) -> httpx.Response:
    """Seul point d'accès réseau (monkeypatché en test)."""
    return HTTP_CLIENT.get(url, headers=HTTP_HEADERS, follow_redirects=True, timeout=30)


def list_day_files(d: date) -> tuple[str | None, list[str]]:
    """Première URL d'index qui répond 200 et ses noms .xml (triés, dédupliqués).

    404 sur toutes les URLs : (None, []). Autre statut : HTTPStatusError.
    """
    for url in day_index_urls(d):
        response = http_get(url)
        if response.status_code == 404:
            continue
        if response.status_code != 200:
            raise httpx.HTTPStatusError(
                f"HTTP {response.status_code} sur {url}",
                request=httpx.Request("GET", url),
                response=response,
            )
        return url, sorted(set(_XML_HREF_RE.findall(response.text)))
    return None, []


def download_xml(base_url: str, filename: str) -> bytes:
    url = base_url + filename
    response = http_get(url)
    if response.status_code != 200:
        raise httpx.HTTPStatusError(
            f"HTTP {response.status_code} sur {url}",
            request=httpx.Request("GET", url),
            response=response,
        )
    return response.content

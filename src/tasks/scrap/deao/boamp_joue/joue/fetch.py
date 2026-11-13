"""Accès réseau à l'API et au site TED pour la source JOUE (issue #165)."""

from datetime import date

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src.config import (
    HTTP_CLIENT,
    HTTP_HEADERS,
    JOUE_ATTENTE_429_S,
    JOUE_COUNTRY_CODES,
    JOUE_INTERVALLE_REQUETES_S,
    TED_API_URL,
    TED_NOTICE_XML_URL,
)
from src.tasks.scrap.deao.limiteur import Limiteur

PAGE_SIZE = 250

_LIMITEUR = Limiteur(JOUE_INTERVALLE_REQUETES_S)


def _post_raw(url: str, payload: dict) -> httpx.Response:
    _LIMITEUR.attendre()
    return HTTP_CLIENT.post(url, json=payload, headers=HTTP_HEADERS, timeout=60)


def _get_raw(url: str) -> httpx.Response:
    _LIMITEUR.attendre()
    return HTTP_CLIENT.get(url, headers=HTTP_HEADERS, follow_redirects=True, timeout=60)


def _verifier(response: httpx.Response, method: str, url: str) -> httpx.Response:
    if response.status_code != 200:
        raise httpx.HTTPStatusError(
            f"HTTP {response.status_code} sur {url}",
            request=httpx.Request(method, url),
            response=response,
        )
    return response


def _a_reessayer(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


_retry = retry(
    retry=retry_if_exception(_a_reessayer),
    stop=stop_after_attempt(6),
    # Pas de Retry-After chez TED : attente croissante à partir de JOUE_ATTENTE_429_S.
    wait=wait_exponential(
        multiplier=JOUE_ATTENTE_429_S, min=JOUE_ATTENTE_429_S, max=60
    ),
    reraise=True,
)


@_retry
def http_post(url: str, payload: dict) -> httpx.Response:
    """Seul accès réseau en POST (monkeypatcher _post_raw en test)."""
    return _verifier(_post_raw(url, payload), "POST", url)


@_retry
def http_get(url: str) -> httpx.Response:
    """Seul accès réseau en GET (monkeypatcher _get_raw en test)."""
    return _verifier(_get_raw(url), "GET", url)


def query_jour(d: date) -> str:
    if not JOUE_COUNTRY_CODES:
        raise ValueError("JOUE_COUNTRY_CODES est vide : aucun pays à interroger.")
    codes = " ".join(JOUE_COUNTRY_CODES)
    return (
        f"(buyer-country IN ({codes}) OR place-of-performance IN ({codes}))"
        f" AND publication-date={d:%Y%m%d}"
    )


def _rechercher(query: str) -> list[str]:
    """Numéros de publication de toutes les pages d'une recherche."""
    payload = {
        "query": query,
        "fields": ["publication-number"],
        "limit": PAGE_SIZE,
        "scope": "ALL",
        "paginationMode": "ITERATION",
    }
    numeros: list[str] = []
    while True:
        data = http_post(f"{TED_API_URL}/notices/search", payload).json()
        notices = data.get("notices") or []
        if not notices:
            break
        numeros.extend(n["publication-number"] for n in notices)
        token = data.get("iterationNextToken")
        if not token:
            break
        payload = {**payload, "iterationNextToken": token}
    return numeros


def list_day_files(d: date) -> tuple[None, list[str]]:
    """Noms `<numéro de publication>.xml` des avis du jour (triés, dédupliqués)."""
    return None, sorted({f"{pn}.xml" for pn in _rechercher(query_jour(d))})


def download_xml(base: str | None, name: str) -> bytes:
    url = TED_NOTICE_XML_URL.format(publication_number=name.removesuffix(".xml"))
    return http_get(url).content


def numero_publication(notice_id: str) -> str | None:
    """Numéro de publication d'un avis à partir de son identifiant eForms."""
    numeros = _rechercher(f"notice-identifier={notice_id}")
    return numeros[0].lstrip("0") if numeros else None

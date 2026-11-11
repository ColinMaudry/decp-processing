"""Télécharge les avis DILA servant de fixtures aux tests DEAO (issue #178).

Les éléments HTML et HTMLSYNTHESE (blobs volumineux, ignorés par le parsing) sont
supprimés. Usage : uv run python script/get_deao_fixtures.py
"""

from pathlib import Path

import httpx
from lxml import etree

BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/BOAMP"
OUTPUT_DIR = Path(__file__).parent.parent / "tests" / "data" / "deao"

FIXTURES = {
    "26-87868": "2026-09-12",
    "26-99074": "2026-10-14",
    "26-92574": "2026-09-24",
    "26-99891": "2026-10-14",
    "26-99177": "2026-10-13",
    "26-99542": "2026-10-14",
    "26-99870": "2026-10-14",
    "26-98580": "2026-10-13",
    "26-98723": "2026-10-14",
    "26-97189": "2026-10-09",
    "26-94806": "2026-10-02",
    "26-98989": "2026-10-14",
    "26-99072": "2026-10-14",
}
STRIPPED = {"HTML", "HTMLSYNTHESE"}


def strip_blobs(content: bytes) -> bytes:
    root = etree.fromstring(content)
    for el in list(root.iter()):
        if isinstance(el.tag, str) and etree.QName(el).localname in STRIPPED:
            el.getparent().remove(el)
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=None)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for idweb, date in FIXTURES.items():
        annee, mois, jour = date.split("-")
        url = f"{BASE_URL}/{annee}/{mois}/{jour}/{idweb}.xml"
        response = httpx.get(url, timeout=60, follow_redirects=True)
        response.raise_for_status()
        (OUTPUT_DIR / f"{idweb}.xml").write_bytes(strip_blobs(response.content))
        print(f"{idweb}.xml")


if __name__ == "__main__":
    main()

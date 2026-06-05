# BOAMP / eForms Flattening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new Prefect flow `boamp_processing` that downloads BOAMP/eForms XML announcements from the DILA archive, flattens them to one row per lot, and publishes a single parquet resource to data.gouv.fr.

**Architecture:** A generic lxml flattener turns each announcement into rows (one per `ProcurementProjectLot`/`lots/lot`, or one row if no lot). Every leaf path becomes a `List[str]` column; a small typed "core" of harmonised columns is extracted on top. Inclusion is decided by a single common metadata field `GESTION/REFERENCE/TYPE_AVIS/NATURE`. Acquisition is a one-time `tar.gz` backfill for 2024-2025 plus an incremental day-crawl for 2026+. Output is concatenated and sunk to parquet/CSV, then published.

**Tech Stack:** Python 3.10+, lxml, polars (lazy/streaming), Prefect 3, httpx (`stream_get`), pytest. All already in `pyproject.toml`.

**Spec:** `docs/superpowers/specs/2026-06-05-boamp-eforms-flattening-design.md`

**Conventions:**

- Imports inside `src/` are absolute from the package root (`from src.tasks.boamp.flatten import ...`).
- Commit messages reference issue `#174`.
- Run a single test with `pytest tests/test_boamp_flatten.py::test_name -v`.

**Key design decisions locked in:**

- Generic leaf-path columns are **always `List[str]`** (length-1 list for single values) so the schema is stable across files/families. The typed core columns are separate scalars.
- Column names = local element path joined by `_`, rooted at `ann` for notice-level (e.g. `GESTION_REFERENCE_IDWEB`, `DONNEES_MAPA_initial_description_objet`) and at the lot container for lot-level (e.g. `ProcurementProjectLot_...`, `lot_...`). No namespace prefixes, no format prefix.
- Attributes are NOT captured in v1 (leaf text only); the value of codelist elements lives in the text. Documented limitation.
- `id_annonce` is prefixed `ef_` (eForms) / `bo_` (legacy); logical row key = `id_annonce` + `lot_id`.

---

## File Structure

**Create:**

- `src/tasks/boamp/__init__.py` — empty package marker.
- `src/tasks/boamp/flatten.py` — pure flattening + core extraction + frame building (no I/O).
- `src/tasks/boamp/fetch.py` — DILA acquisition (day index, download, date range, tar.gz backfill).
- `src/flows/boamp_processing.py` — the Prefect flow + per-day / per-year tasks.
- `tests/test_boamp_flatten.py`, `tests/test_boamp_fetch.py`, `tests/test_boamp_flow.py`.
- `tests/data/boamp/` — XML fixtures (downloaded from DILA in Task 7).

**Modify:**

- `src/config.py` — BOAMP configuration block.
- `src/tasks/publish.py` — add `publish_boamp_to_datagouv`.
- `run_flow.py` — register `boamp_processing`.
- `src/deployments.py` — add `boamp-processing` deployment.
- `template.env` — document new env vars.

---

## Task 1: Configuration

**Files:**

- Modify: `src/config.py` (append a BOAMP block near the end, before `check_s3_config`)
- Modify: `template.env`

- [ ] **Step 1: Add the BOAMP config block**

Append to `src/config.py` (after the `ACHETEURS_NON_SIRENE` block, before `def check_s3_config`):

```python
# ---------------------------------------------------------------------------
# BOAMP / eForms (flow boamp_processing) — issue #174
# ---------------------------------------------------------------------------

BOAMP_BASE_URL = os.getenv(
    "BOAMP_BASE_URL", "https://echanges.dila.gouv.fr/OPENDATA/BOAMP"
)

# Première date traitée (incluse). Les fichiers < cette date sont ignorés.
BOAMP_START_DATE = os.getenv("BOAMP_START_DATE", "2024-01-01")

# Années traitées en backfill via les archives .tar.gz de FluxHistorique.
BOAMP_BACKFILL_YEARS = [
    int(y) for y in os.getenv("BOAMP_BACKFILL_YEARS", "2024,2025").split(",") if y
]

# Année à partir de laquelle le flux est servi dans /OPENDATA/BOAMP/<année>/
# (et non FluxHistorique). Les années < cette valeur sont en FluxHistorique.
BOAMP_ARCHIVE_CUTOFF_YEAR = int(os.getenv("BOAMP_ARCHIVE_CUTOFF_YEAR", "2026"))

# Archives bulk téléchargées pour le backfill (uniquement les appels d'offres "AO" ;
# les "IC-AA" = avis d'attribution, exclus du périmètre).
BOAMP_BACKFILL_ARCHIVES = os.getenv(
    "BOAMP_BACKFILL_ARCHIVES", "BOAMP-J-AO,BOAMP-N-AO,MAPA-AO"
).split(",")

# Nombre de derniers jours re-crawlés à chaque run (rattrapage des ajouts tardifs).
BOAMP_RECRAWL_DAYS = int(os.getenv("BOAMP_RECRAWL_DAYS", "7"))

# Filtrage par NATURE (cf. spec). Inclusions directes :
BOAMP_INCLUDED_NATURES = {"APPEL_OFFRE", "QUALIFICATION", "RECTIFICATIF", "MODIFICATION"}
# Inclusions conditionnelles : ces NATURE ne sont gardées que si le sous-élément
# est MISE_EN_CONCURRENCE (avis valant appel à la concurrence).
BOAMP_MISE_EN_CONCURRENCE_NATURES = {"PRE-INFORMATION", "PERIODIQUE"}

# URL publique d'un avis (construite depuis l'IDWEB).
BOAMP_AVIS_URL_TEMPLATE = os.getenv(
    "BOAMP_AVIS_URL_TEMPLATE", 'https://www.boamp.fr/pages/avis/?q=idweb:"{idweb}"'
)

# Dossiers de travail / sortie.
BOAMP_DATA_DIR = make_path_from_env("BOAMP_DATA_DIR", DATA_DIR / "boamp")
BOAMP_DATA_DIR.mkdir(exist_ok=True, parents=True)
BOAMP_PARTITIONS_DIR = BOAMP_DATA_DIR / "partitions"
BOAMP_PARTITIONS_DIR.mkdir(exist_ok=True, parents=True)
ALL_CONFIG["BOAMP_DATA_DIR"] = BOAMP_DATA_DIR

# Publication data.gouv.fr (à renseigner une fois le dataset créé).
BOAMP_DATASET_ID = os.getenv("BOAMP_DATASET_ID", "")
BOAMP_PARQUET_RESOURCE_ID = os.getenv("BOAMP_PARQUET_RESOURCE_ID", "")
BOAMP_CSV_RESOURCE_ID = os.getenv("BOAMP_CSV_RESOURCE_ID", "")
```

- [ ] **Step 2: Document env vars in `template.env`**

Append:

```bash
# BOAMP / eForms (flow boamp_processing) — issue #174
BOAMP_START_DATE=2024-01-01
BOAMP_BACKFILL_YEARS=2024,2025
BOAMP_RECRAWL_DAYS=7
# À renseigner après création du dataset data.gouv.fr :
BOAMP_DATASET_ID=
BOAMP_PARQUET_RESOURCE_ID=
BOAMP_CSV_RESOURCE_ID=
```

- [ ] **Step 3: Verify config imports cleanly**

Run: `python -c "import src.config as c; print(c.BOAMP_INCLUDED_NATURES, c.BOAMP_PARTITIONS_DIR)"`
Expected: prints the set and a path ending in `data/boamp/partitions`.

- [ ] **Step 4: Commit**

```bash
git add src/config.py template.env
git commit -m "feat(boamp): configuration du flow boamp_processing (#174)"
```

---

## Task 2: Generic leaf-path flattener

**Files:**

- Create: `src/tasks/boamp/__init__.py`
- Create: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Create the empty package marker**

Create `src/tasks/boamp/__init__.py` with no content (empty file).

- [ ] **Step 2: Write the failing test**

Create `tests/test_boamp_flatten.py`:

```python
from lxml import etree

from src.tasks.boamp.flatten import flatten, localname


def _el(xml: str):
    return etree.fromstring(xml.encode("utf-8"))


def test_localname_strips_namespace():
    el = _el('<a xmlns="urn:x"><b>1</b></a>')
    assert localname(el.tag) == "a"


def test_flatten_single_leaf_is_length_one_list():
    el = _el("<root><a>x</a></root>")
    assert flatten(el) == {"root_a": ["x"]}


def test_flatten_repeated_leaf_accumulates():
    el = _el("<root><a>x</a><a>y</a></root>")
    assert flatten(el) == {"root_a": ["x", "y"]}


def test_flatten_nested_paths_and_skips_empty_text():
    el = _el("<root><g><a>x</a><b></b></g></root>")
    assert flatten(el) == {"root_g_a": ["x"]}


def test_flatten_skip_localnames():
    el = _el("<root><keep>1</keep><HTML>big</HTML></root>")
    assert flatten(el, skip_localnames={"HTML"}) == {"root_keep": ["1"]}


def test_flatten_start_path_override():
    el = _el("<lot><numLot>4</numLot></lot>")
    assert flatten(el, start_path="lot") == {"lot_numLot": ["4"]}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.tasks.boamp.flatten'`.

- [ ] **Step 4: Implement the flattener**

Create `src/tasks/boamp/flatten.py`:

```python
"""Aplatissement générique des annonces BOAMP/eForms (issue #174).

Aucune I/O : fonctions pures sur des éléments lxml.
"""

from lxml import etree


def localname(tag) -> str:
    """Nom local d'une balise lxml (sans préfixe de namespace)."""
    if not isinstance(tag, str):  # commentaires, PI…
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def flatten(
    elem,
    start_path: str = "",
    skip_localnames: frozenset | set = frozenset(),
) -> dict[str, list[str]]:
    """Aplatit un sous-arbre en {chemin_feuille: [valeurs...]}.

    - chemin = noms locaux joints par "_", préfixé par ``start_path`` ;
    - seules les feuilles (sans enfant élément) avec texte non vide sont gardées ;
    - les feuilles répétées accumulent plusieurs valeurs ;
    - les sous-arbres dont le nom local est dans ``skip_localnames`` sont ignorés ;
    - les attributs ne sont pas capturés en v1.
    """
    acc: dict[str, list[str]] = {}

    def walk(e, path: str):
        children = [c for c in e if isinstance(c.tag, str)]
        if not children:
            text = (e.text or "").strip()
            if text:
                acc.setdefault(path, []).append(text)
            return
        for child in children:
            name = localname(child.tag)
            if name in skip_localnames:
                continue
            child_path = f"{path}_{name}" if path else name
            walk(child, child_path)

    walk(elem, start_path)
    return acc
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/tasks/boamp/__init__.py src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): aplatisseur générique chemin-feuille -> colonnes liste (#174)"
```

---

## Task 3: Format detection, TYPE_AVIS parsing, inclusion filter

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
from src.tasks.boamp.flatten import detect_format, is_included, parse_type_avis

ANN_TPL = """
<ann>
  <DONNEES>{donnees}</DONNEES>
  <GESTION><REFERENCE><IDWEB>26-1</IDWEB>
    <TYPE_AVIS>
      <FAMILLE><{famille}/></FAMILLE>
      <NATURE>{nature}</NATURE>
      <STATUT><{statut}/></STATUT>
    </TYPE_AVIS>
  </REFERENCE></GESTION>
</ann>
"""


def _ann(donnees, nature_xml, famille="MAPA", statut="INITIAL"):
    return _el(
        ANN_TPL.format(donnees=donnees, nature=nature_xml, famille=famille, statut=statut)
    )


def test_detect_format_eforms_vs_legacy():
    assert detect_format(_ann("<EFORMS/>", "<APPEL_OFFRE/>")) == "eforms"
    assert detect_format(_ann("<MAPA/>", "<APPEL_OFFRE/>")) == "boamp"


def test_parse_type_avis_returns_nature_sub_statut_famille():
    nature, sub, statut, famille = parse_type_avis(_ann("<MAPA/>", "<APPEL_OFFRE><STANDARD/></APPEL_OFFRE>"))
    assert (nature, sub, statut, famille) == ("APPEL_OFFRE", "STANDARD", "INITIAL", "MAPA")


def test_is_included_whitelist():
    assert is_included("APPEL_OFFRE", None) is True
    assert is_included("QUALIFICATION", "STANDARD") is True
    assert is_included("ATTRIBUTION", "STANDARD") is False
    assert is_included("PROFIL_ACHETEUR", None) is False


def test_is_included_pre_information_only_when_mise_en_concurrence():
    assert is_included("PRE-INFORMATION", "MISE_EN_CONCURRENCE") is True
    assert is_included("PERIODIQUE", "MISE_EN_CONCURRENCE") is True
    assert is_included("PRE-INFORMATION", "STANDARD") is False
    assert is_included("PERIODIQUE", None) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k "detect_format or type_avis or is_included" -v`
Expected: FAIL with `ImportError: cannot import name 'detect_format'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/flatten.py` (top: add the config import; bottom: the functions):

```python
from src.config import (
    BOAMP_INCLUDED_NATURES,
    BOAMP_MISE_EN_CONCURRENCE_NATURES,
)


def _find(elem, name: str):
    """Premier descendant (ou self) dont le nom local == name, sinon None."""
    if localname(elem.tag) == name:
        return elem
    for e in elem.iter():
        if localname(e.tag) == name:
            return e
    return None


def _first_child_localname(elem) -> str | None:
    """Nom local du premier enfant élément, sinon None."""
    for c in elem:
        if isinstance(c.tag, str):
            return localname(c.tag)
    return None


def detect_format(root) -> str:
    """'eforms' si un élément EFORMS est présent, sinon 'boamp' (legacy v240)."""
    return "eforms" if _find(root, "EFORMS") is not None else "boamp"


def parse_type_avis(root):
    """(nature, sous_nature, statut, famille) depuis GESTION/REFERENCE/TYPE_AVIS.

    Chaque valeur est le nom local de l'enfant (ex. NATURE -> 'APPEL_OFFRE',
    sous-nature = enfant de cet enfant -> 'MISE_EN_CONCURRENCE'), ou None.
    """
    type_avis = _find(root, "TYPE_AVIS")
    if type_avis is None:
        return None, None, None, None
    nature_el = _find(type_avis, "NATURE")
    nature = _first_child_localname(nature_el) if nature_el is not None else None
    sub = None
    if nature_el is not None:
        nature_child = next((c for c in nature_el if isinstance(c.tag, str)), None)
        if nature_child is not None:
            sub = _first_child_localname(nature_child)
    famille_el = _find(type_avis, "FAMILLE")
    famille = _first_child_localname(famille_el) if famille_el is not None else None
    statut_el = _find(type_avis, "STATUT")
    statut = _first_child_localname(statut_el) if statut_el is not None else None
    return nature, sub, statut, famille


def is_included(nature: str | None, sous_nature: str | None) -> bool:
    """Décide si l'avis entre dans le périmètre (liste blanche NATURE)."""
    if nature in BOAMP_INCLUDED_NATURES:
        return True
    if nature in BOAMP_MISE_EN_CONCURRENCE_NATURES and sous_nature == "MISE_EN_CONCURRENCE":
        return True
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: PASS (all tests, including Task 2's).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): détection format + parsing TYPE_AVIS + filtre NATURE (#174)"
```

---

## Task 4: Lot discovery and lot-level core

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
from src.tasks.boamp.flatten import extract_lot_core, find_lots

EFORMS_LOT = """
<ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b">
  <cac:ProcurementProjectLot>
    <cbc:ID schemeName="Lot">LOT-0001</cbc:ID>
    <cac:ProcurementProject><cbc:Name>Intitulé A</cbc:Name></cac:ProcurementProject>
  </cac:ProcurementProjectLot>
  <cac:ProcurementProjectLot>
    <cbc:ID schemeName="Lot">LOT-0002</cbc:ID>
    <cac:ProcurementProject><cbc:Name>Intitulé B</cbc:Name></cac:ProcurementProject>
  </cac:ProcurementProjectLot>
</ContractNotice>
"""

LEGACY_LOTS = """
<MAPA><initial><lots>
  <lot><numLot>4</numLot><description>Métal</description></lot>
  <lot><numLot>5</numLot><description>Bois</description></lot>
</lots></initial></MAPA>
"""


def test_find_lots_eforms():
    lots = find_lots(_el(EFORMS_LOT), "eforms")
    assert len(lots) == 2


def test_find_lots_legacy():
    lots = find_lots(_el(LEGACY_LOTS), "boamp")
    assert len(lots) == 2


def test_extract_lot_core_eforms():
    lots = find_lots(_el(EFORMS_LOT), "eforms")
    assert extract_lot_core(lots[0], "eforms") == {
        "lot_id": "LOT-0001",
        "lot_intitule": "Intitulé A",
    }


def test_extract_lot_core_legacy():
    lots = find_lots(_el(LEGACY_LOTS), "boamp")
    assert extract_lot_core(lots[1], "boamp") == {
        "lot_id": "5",
        "lot_intitule": "Bois",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k "find_lots or lot_core" -v`
Expected: FAIL with `ImportError: cannot import name 'find_lots'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/flatten.py`:

```python
LOT_CONTAINER = {"eforms": "ProcurementProjectLot", "boamp": "lot"}


def _text(elem) -> str | None:
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def find_lots(root, source_format: str) -> list:
    """Liste des éléments-lot selon le format."""
    if source_format == "eforms":
        return [e for e in root.iter() if localname(e.tag) == "ProcurementProjectLot"]
    lots = []
    for container in root.iter():
        if localname(container.tag) == "lots":
            lots.extend(c for c in container if localname(c.tag) == "lot")
    return lots


def extract_lot_core(lot, source_format: str) -> dict:
    """{lot_id, lot_intitule} pour un élément-lot."""
    if source_format == "eforms":
        lot_id = None
        for e in lot.iter():
            if localname(e.tag) == "ID" and e.get("schemeName") == "Lot":
                lot_id = _text(e)
                break
        name_el = next((e for e in lot.iter() if localname(e.tag) == "Name"), None)
        return {"lot_id": lot_id, "lot_intitule": _text(name_el)}
    # legacy
    num = next((c for c in lot if localname(c.tag) == "numLot"), None)
    desc = next((c for c in lot if localname(c.tag) == "description"), None)
    return {"lot_id": _text(num), "lot_intitule": _text(desc)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): découverte des lots + cœur lot (id, intitulé) (#174)"
```

---

## Task 5: eForms buyer de-referencing

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
from src.tasks.boamp.flatten import eforms_acheteur

EFORMS_BUYER = """
<ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b" xmlns:efac="urn:e">
  <ext:UBLExtensions xmlns:ext="urn:x"><efac:Organizations>
    <efac:Organization><efac:Company>
      <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0001</cbc:ID></cac:PartyIdentification>
      <cac:PartyName><cbc:Name>Hauts-de-Seine Habitat</cbc:Name></cac:PartyName>
      <cac:PostalAddress><cbc:CityName>Levallois</cbc:CityName></cac:PostalAddress>
      <cac:PartyLegalEntity><cbc:CompanyID>27920022400012</cbc:CompanyID></cac:PartyLegalEntity>
    </efac:Company></efac:Organization>
    <efac:Organization><efac:Company>
      <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0000</cbc:ID></cac:PartyIdentification>
      <cac:PartyName><cbc:Name>Plateforme eSender</cbc:Name></cac:PartyName>
    </efac:Company></efac:Organization>
  </efac:Organizations></ext:UBLExtensions>
  <cac:ContractingParty><cac:Party>
    <cac:PartyIdentification><cbc:ID>ORG-0001</cbc:ID></cac:PartyIdentification>
    <cac:ServiceProviderParty><cac:Party>
      <cac:PartyIdentification><cbc:ID>ORG-0000</cbc:ID></cac:PartyIdentification>
    </cac:Party></cac:ServiceProviderParty>
  </cac:Party></cac:ContractingParty>
</ContractNotice>
"""


def test_eforms_acheteur_resolves_buyer_not_service_provider():
    res = eforms_acheteur(_el(EFORMS_BUYER))
    assert res == {
        "acheteur_nom": "Hauts-de-Seine Habitat",
        "acheteur_siret": "27920022400012",
        "acheteur_ville": "Levallois",
    }


def test_eforms_acheteur_empty_when_no_contracting_party():
    res = eforms_acheteur(_el('<ContractNotice xmlns:cac="urn:c"/>'))
    assert res == {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k eforms_acheteur -v`
Expected: FAIL with `ImportError: cannot import name 'eforms_acheteur'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/flatten.py`:

```python
def _direct_child(elem, name: str):
    return next((c for c in elem if localname(c.tag) == name), None)


def _buyer_org_id(root) -> str | None:
    """ID de l'organisation acheteuse = PartyIdentification/ID enfant DIRECT de
    ContractingParty/Party (et non du ServiceProviderParty imbriqué)."""
    cp = _find(root, "ContractingParty")
    if cp is None:
        return None
    party = _direct_child(cp, "Party")
    if party is None:
        return None
    pid = _direct_child(party, "PartyIdentification")
    if pid is None:
        return None
    return _text(_direct_child(pid, "ID"))


def _find_company_by_id(root, org_id: str):
    """efac:Company dont PartyIdentification/ID == org_id."""
    for org in root.iter():
        if localname(org.tag) != "Organization":
            continue
        company = _find(org, "Company")
        if company is None:
            continue
        pid = _find(company, "PartyIdentification")
        if pid is None:
            continue
        idv = next((e for e in pid.iter() if localname(e.tag) == "ID"), None)
        if _text(idv) == org_id:
            return company
    return None


def eforms_acheteur(root) -> dict:
    """{acheteur_nom, acheteur_siret, acheteur_ville} par déréférencement interne."""
    empty = {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}
    org_id = _buyer_org_id(root)
    if org_id is None:
        return empty
    company = _find_company_by_id(root, org_id)
    if company is None:
        return empty
    name_el = _find(_find(company, "PartyName") or company, "Name")
    siret_el = _find(_find(company, "PartyLegalEntity") or company, "CompanyID")
    city_el = _find(_find(company, "PostalAddress") or company, "CityName")
    return {
        "acheteur_nom": _text(name_el),
        "acheteur_siret": _text(siret_el),
        "acheteur_ville": _text(city_el),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -k eforms_acheteur -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): déréférencement acheteur eForms (nom, SIRET, ville) (#174)"
```

---

## Task 6: Harmonised core extraction

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
from src.tasks.boamp.flatten import extract_core

LEGACY_FULL = """
<ann>
  <DONNEES><MAPA><organisme>
      <acheteurPublic>Ville de X</acheteurPublic>
      <adr><ville>Paristown</ville></adr>
    </organisme>
    <initial><description><objet>Travaux divers</objet></description></initial>
  </MAPA></DONNEES>
  <GESTION><REFERENCE><IDWEB>26-10979</IDWEB>
    <TYPE_AVIS><FAMILLE><MAPA/></FAMILLE><NATURE><APPEL_OFFRE><STANDARD/></APPEL_OFFRE></NATURE>
      <STATUT><INITIAL/></STATUT></TYPE_AVIS>
  </REFERENCE>
  <INDEXATION><DATE_PUBLICATION>2026-01-31</DATE_PUBLICATION>
    <DATE_LIMITE_REPONSE>2026-03-30T22:00:00</DATE_LIMITE_REPONSE></INDEXATION>
  </GESTION>
</ann>
"""


def test_extract_core_legacy():
    root = _el(LEGACY_FULL)
    core = extract_core(root, "boamp", "26-10979.xml")
    assert core["id_annonce"] == "bo_26-10979"
    assert core["source_format"] == "boamp"
    assert core["nature_avis"] == "APPEL_OFFRE"
    assert core["statut_avis"] == "INITIAL"
    assert core["famille_avis"] == "MAPA"
    assert core["type_document"] == "MAPA"
    assert core["sous_type_eforms"] is None
    assert core["acheteur_nom"] == "Ville de X"
    assert core["acheteur_ville"] == "Paristown"
    assert core["objet"] == "Travaux divers"
    assert core["date_publication"] == "2026-01-31"
    assert core["date_limite_reponse"] == "2026-03-30T22:00:00"
    assert core["url_avis"] == 'https://www.boamp.fr/pages/avis/?q=idweb:"26-10979"'
    assert core["fichier_source"] == "26-10979.xml"
    assert core["lot_id"] is None and core["lot_intitule"] is None


def test_extract_core_eforms_id_prefix_and_subtype():
    xml = """
    <ann><DONNEES><EFORMS><ContractNotice xmlns:cac="urn:c" xmlns:cbc="urn:b" xmlns:efac="urn:e" xmlns:efbc="urn:f">
        <ext:UBLExtensions xmlns:ext="urn:x"><efac:NoticeSubType>
          <cbc:SubTypeCode listName="notice-subtype">16</cbc:SubTypeCode></efac:NoticeSubType></ext:UBLExtensions>
        <cac:ProcurementProject><cbc:Name>Objet global</cbc:Name></cac:ProcurementProject>
      </ContractNotice></EFORMS></DONNEES>
      <GESTION><REFERENCE><IDWEB>26-100</IDWEB>
        <TYPE_AVIS><FAMILLE><JOUE/></FAMILLE><NATURE><APPEL_OFFRE/></NATURE><STATUT><INITIAL/></STATUT></TYPE_AVIS>
      </REFERENCE><INDEXATION><DATE_PUBLICATION>2026-02-01</DATE_PUBLICATION></INDEXATION></GESTION>
    </ann>
    """
    core = extract_core(_el(xml), "eforms", "26-100.xml")
    assert core["id_annonce"] == "ef_26-100"
    assert core["type_document"] == "ContractNotice"
    assert core["sous_type_eforms"] == "16"
    assert core["objet"] == "Objet global"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k extract_core -v`
Expected: FAIL with `ImportError: cannot import name 'extract_core'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/flatten.py` (add the two config imports to the existing config import block):

```python
from src.config import (
    BOAMP_AVIS_URL_TEMPLATE,
    BOAMP_INCLUDED_NATURES,
    BOAMP_MISE_EN_CONCURRENCE_NATURES,
)

CORE_KEYS = (
    "source_format", "id_annonce", "nature_avis", "statut_avis", "famille_avis",
    "type_document", "sous_type_eforms", "date_publication", "date_limite_reponse",
    "acheteur_nom", "acheteur_siret", "acheteur_ville", "objet", "url_avis",
    "fichier_source", "lot_id", "lot_intitule",
)


def _gestion_text(root, name: str) -> str | None:
    gestion = _find(root, "GESTION")
    if gestion is None:
        return None
    return _text(_find(gestion, name))


def _legacy_objet(root):
    donnees = _find(root, "DONNEES")
    return _text(_find(donnees, "objet")) if donnees is not None else None


def _eforms_objet(root):
    """Name du ProcurementProject de niveau annonce (enfant direct du ContractNotice)."""
    cn = _find(root, "ContractNotice")
    if cn is None:
        return None
    pp = _direct_child(cn, "ProcurementProject")
    return _text(_find(pp, "Name")) if pp is not None else None


def _legacy_acheteur(root) -> dict:
    organisme = _find(root, "organisme")
    if organisme is None:
        return {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}
    return {
        "acheteur_nom": _text(_find(organisme, "acheteurPublic")),
        "acheteur_siret": None,
        "acheteur_ville": _text(_find(organisme, "ville")),
    }


def extract_core(root, source_format: str, fichier_source: str) -> dict:
    """Cœur harmonisé (colonnes scalaires) pour une annonce."""
    nature, _sub, statut, famille = parse_type_avis(root)
    idweb = _gestion_text(root, "IDWEB")
    prefix = "ef_" if source_format == "eforms" else "bo_"
    id_annonce = f"{prefix}{idweb}" if idweb else None

    if source_format == "eforms":
        acheteur = eforms_acheteur(root)
        objet = _eforms_objet(root)
        type_document = _first_child_localname(_find(root, "EFORMS"))
        sous_type_eforms = _text(_find(root, "SubTypeCode"))
    else:
        acheteur = _legacy_acheteur(root)
        objet = _legacy_objet(root)
        type_document = _first_child_localname(_find(root, "DONNEES"))
        sous_type_eforms = None

    return {
        "source_format": source_format,
        "id_annonce": id_annonce,
        "nature_avis": nature,
        "statut_avis": statut,
        "famille_avis": famille,
        "type_document": type_document,
        "sous_type_eforms": sous_type_eforms,
        "date_publication": _gestion_text(root, "DATE_PUBLICATION"),
        "date_limite_reponse": _gestion_text(root, "DATE_LIMITE_REPONSE"),
        "acheteur_nom": acheteur["acheteur_nom"],
        "acheteur_siret": acheteur["acheteur_siret"],
        "acheteur_ville": acheteur["acheteur_ville"],
        "objet": objet,
        "url_avis": BOAMP_AVIS_URL_TEMPLATE.format(idweb=idweb) if idweb else None,
        "fichier_source": fichier_source,
        "lot_id": None,
        "lot_intitule": None,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -k extract_core -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): extraction du cœur harmonisé (deux formats) (#174)"
```

---

## Task 7: Row assembly (`annonce_to_rows`) + real fixtures

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Create: `tests/data/boamp/*.xml`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Download real fixtures from DILA**

Run:

```bash
mkdir -p tests/data/boamp
base="https://echanges.dila.gouv.fr/OPENDATA/BOAMP/2026/01/31"
curl -s "$base/26-10255.xml" -o tests/data/boamp/eforms_contract_notice.xml   # ContractNotice / APPEL_OFFRE
curl -s "$base/26-10260.xml" -o tests/data/boamp/eforms_award_notice.xml      # ContractAwardNotice / ATTRIBUTION (exclu)
curl -s "$base/26-10979.xml" -o tests/data/boamp/legacy_mapa.xml              # MAPA / APPEL_OFFRE
ls -la tests/data/boamp/
```

Expected: three `.xml` files present (sizes ~30-90K). If the network is unavailable, copy them from `/tmp/boamp_sample/` if present.

- [ ] **Step 2: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
from pathlib import Path

from src.tasks.boamp.flatten import annonce_to_rows

FIXTURES = Path(__file__).parent / "data" / "boamp"


def _parse_fixture(name):
    return etree.parse(str(FIXTURES / name)).getroot()


def test_annonce_to_rows_excludes_attribution():
    root = _parse_fixture("eforms_award_notice.xml")
    assert annonce_to_rows(root, "eforms_award_notice.xml") == []


def test_annonce_to_rows_eforms_one_row_per_lot():
    root = _parse_fixture("eforms_contract_notice.xml")
    rows = annonce_to_rows(root, "eforms_contract_notice.xml")
    assert len(rows) >= 1
    r = rows[0]
    assert r["source_format"] == "eforms"
    assert r["id_annonce"].startswith("ef_")
    assert r["nature_avis"] == "APPEL_OFFRE"
    assert r["acheteur_siret"] is not None  # SIRET déréférencé
    assert r["lot_id"] is not None
    # colonnes génériques = listes
    list_cols = [k for k, v in r.items() if isinstance(v, list)]
    assert list_cols, "au moins une colonne générique de type liste"


def test_annonce_to_rows_legacy_lot_grain():
    root = _parse_fixture("legacy_mapa.xml")
    rows = annonce_to_rows(root, "legacy_mapa.xml")
    assert len(rows) >= 1
    assert rows[0]["source_format"] == "boamp"
    assert rows[0]["id_annonce"] == "bo_26-10979"
    # le lot legacy 26-10979 a numLot=4
    assert any(r["lot_id"] == "4" for r in rows)


def test_annonce_to_rows_no_lot_single_row():
    xml = """
    <ann><DONNEES><MAPA><initial><description><objet>X</objet></description></initial></MAPA></DONNEES>
    <GESTION><REFERENCE><IDWEB>26-9</IDWEB><TYPE_AVIS><FAMILLE><MAPA/></FAMILLE>
      <NATURE><APPEL_OFFRE/></NATURE><STATUT><INITIAL/></STATUT></TYPE_AVIS></REFERENCE>
      <INDEXATION><DATE_PUBLICATION>2026-01-01</DATE_PUBLICATION></INDEXATION></GESTION></ann>
    """
    rows = annonce_to_rows(_el(xml), "x.xml")
    assert len(rows) == 1
    assert rows[0]["lot_id"] is None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k annonce_to_rows -v`
Expected: FAIL with `ImportError: cannot import name 'annonce_to_rows'`.

- [ ] **Step 4: Implement**

Add to `src/tasks/boamp/flatten.py`:

```python
EXCLUDE_ELEMENTS = frozenset({"HTML", "HTMLSYNTHESE"})


def annonce_to_rows(root, fichier_source: str) -> list[dict]:
    """Transforme une annonce en lignes (une par lot, ou une seule si pas de lot).

    Retourne [] si l'avis est hors périmètre (filtre NATURE).
    """
    source_format = detect_format(root)
    nature, sous_nature, _statut, _famille = parse_type_avis(root)
    if not is_included(nature, sous_nature):
        return []

    core = extract_core(root, source_format, fichier_source)
    lot_container = LOT_CONTAINER[source_format]
    notice_cols = flatten(root, skip_localnames=EXCLUDE_ELEMENTS | {lot_container})
    lots = find_lots(root, source_format)

    if not lots:
        return [{**core, **notice_cols}]

    rows = []
    for lot in lots:
        lot_core = extract_lot_core(lot, source_format)
        lot_cols = flatten(lot, start_path=lot_container)
        rows.append({**core, **lot_core, **notice_cols, **lot_cols})
    return rows
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: PASS (all tests).

- [ ] **Step 6: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py tests/data/boamp
git commit -m "feat(boamp): assemblage des lignes (grain lot) + fixtures réelles (#174)"
```

---

## Task 8: Frame building and partition concatenation

**Files:**

- Modify: `src/tasks/boamp/flatten.py`
- Test: `tests/test_boamp_flatten.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_flatten.py`:

```python
import polars as pl

from src.tasks.boamp.flatten import concat_partitions, rows_to_frame


def test_rows_to_frame_lists_and_typed_dates():
    rows = [
        {"id_annonce": "bo_1", "date_publication": "2026-01-31", "GESTION_X": ["a"]},
        {"id_annonce": "bo_2", "date_publication": "2026-02-01", "GESTION_X": ["a", "b"]},
    ]
    df = rows_to_frame(rows)
    assert df.height == 2
    assert df.schema["date_publication"] == pl.Date
    assert df.schema["GESTION_X"] == pl.List(pl.String)


def test_rows_to_frame_empty():
    assert rows_to_frame([]).is_empty()


def test_concat_partitions_diagonal(tmp_path):
    pl.DataFrame({"a": ["1"], "only_x": [["v"]]}).write_parquet(tmp_path / "p1.parquet")
    sub = tmp_path / "2026" / "01"
    sub.mkdir(parents=True)
    pl.DataFrame({"a": ["2"], "only_y": [["w"]]}).write_parquet(sub / "02.parquet")
    lf = concat_partitions(tmp_path)
    df = lf.collect()
    assert set(df.columns) == {"a", "only_x", "only_y"}
    assert df.height == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flatten.py -k "rows_to_frame or concat_partitions" -v`
Expected: FAIL with `ImportError: cannot import name 'rows_to_frame'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/flatten.py` (add `import polars as pl` and `from pathlib import Path` at the top of the file):

```python
DATE_CORE_COLUMNS = ("date_publication", "date_limite_reponse")


def rows_to_frame(rows: list[dict]) -> pl.DataFrame:
    """Construit un DataFrame depuis des lignes hétérogènes.

    - colonnes manquantes -> null ; colonnes génériques -> List(String) ;
    - les colonnes de date du cœur sont castées en pl.Date (non strict).
    """
    if not rows:
        return pl.DataFrame()
    df = pl.from_dicts(rows, infer_schema_length=None)
    for col in DATE_CORE_COLUMNS:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.String).str.slice(0, 10).str.to_date(strict=False)
            )
    return df


def concat_partitions(partitions_dir) -> pl.LazyFrame:
    """Concatène (diagonale) tous les parquets sous partitions_dir, récursivement."""
    paths = sorted(Path(partitions_dir).rglob("*.parquet"))
    if not paths:
        return pl.LazyFrame()
    return pl.concat([pl.scan_parquet(p) for p in paths], how="diagonal_relaxed")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flatten.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/flatten.py tests/test_boamp_flatten.py
git commit -m "feat(boamp): construction du DataFrame + concat des partitions (#174)"
```

---

## Task 9: Acquisition — day index, download, date range

**Files:**

- Create: `src/tasks/boamp/fetch.py`
- Test: `tests/test_boamp_fetch.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_boamp_fetch.py`:

```python
from datetime import date

import src.tasks.boamp.fetch as fetch
from src.tasks.boamp.fetch import day_index_url, iter_days, list_day_files

INDEX_HTML = """
<html><body><pre>
<a href="?C=N;O=D">Name</a>
<a href="/parent/">Parent Directory</a>
<a href="26-10240.xml">26-10240.xml</a> 2026-01-31 14:15 86K
<a href="26-10979.xml">26-10979.xml</a> 2026-01-31 23:45 32K
<a href="26-10240.xml">26-10240.xml</a> dup
</pre></body></html>
"""


def test_day_index_url_routing():
    assert day_index_url(date(2025, 6, 15)).endswith(
        "/FluxHistorique/Boamp_v240/2025/06/15/"
    )
    assert day_index_url(date(2026, 1, 31)).endswith("/2026/01/31/")


def test_iter_days_inclusive():
    days = list(iter_days(date(2026, 1, 30), date(2026, 2, 1)))
    assert days == [date(2026, 1, 30), date(2026, 1, 31), date(2026, 2, 1)]


def test_list_day_files_parses_and_dedupes(monkeypatch):
    monkeypatch.setattr(
        fetch, "stream_get", lambda url, **kw: iter([INDEX_HTML.encode("utf-8")])
    )
    files = list_day_files(date(2026, 1, 31))
    assert files == ["26-10240.xml", "26-10979.xml"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_fetch.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.tasks.boamp.fetch'`.

- [ ] **Step 3: Implement**

Create `src/tasks/boamp/fetch.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_fetch.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/fetch.py tests/test_boamp_fetch.py
git commit -m "feat(boamp): acquisition quotidienne (index, téléchargement, plage) (#174)"
```

---

## Task 10: Acquisition — tar.gz backfill

**Files:**

- Modify: `src/tasks/boamp/fetch.py`
- Test: `tests/test_boamp_fetch.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_boamp_fetch.py`:

```python
import io
import tarfile

from src.tasks.boamp.fetch import iter_archive_xml, year_archive_urls


def test_year_archive_urls_uses_ao_only(monkeypatch):
    urls = year_archive_urls(2024)
    assert all(u.endswith(".tar.gz") for u in urls)
    assert any("MAPA-AO" in u for u in urls)
    assert not any("IC-AA" in u for u in urls)  # attributions exclues


def _make_targz(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def test_iter_archive_xml_yields_xml_members(monkeypatch):
    targz = _make_targz({"a/26-1.xml": b"<ann/>", "a/readme.txt": b"x", "a/26-2.xml": b"<ann/>"})
    monkeypatch.setattr("src.tasks.boamp.fetch.year_archive_urls", lambda y: ["u1"])
    monkeypatch.setattr("src.tasks.boamp.fetch.stream_get", lambda url, **kw: iter([targz]))
    out = list(iter_archive_xml(2024))
    names = sorted(n for n, _ in out)
    assert names == ["a/26-1.xml", "a/26-2.xml"]
    assert all(content == b"<ann/>" for _, content in out)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_fetch.py -k archive -v`
Expected: FAIL with `ImportError: cannot import name 'iter_archive_xml'`.

- [ ] **Step 3: Implement**

Add to `src/tasks/boamp/fetch.py` (add `import tarfile`, `import tempfile`, and import `BOAMP_BACKFILL_ARCHIVES` from config):

```python
import tarfile
import tempfile

from src.config import BOAMP_BACKFILL_ARCHIVES  # add to the existing config import


def year_archive_urls(year: int) -> list[str]:
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_fetch.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/fetch.py tests/test_boamp_fetch.py
git commit -m "feat(boamp): backfill via archives tar.gz (AO uniquement) (#174)"
```

---

## Task 11: The `boamp_processing` flow

**Files:**

- Create: `src/flows/boamp_processing.py`
- Test: `tests/test_boamp_flow.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_boamp_flow.py`:

```python
from datetime import date
from pathlib import Path

import polars as pl
from lxml import etree

import src.flows.boamp_processing as flowmod
from src.flows.boamp_processing import process_day

FIXTURES = Path(__file__).parent / "data" / "boamp"


def test_process_day_writes_partition_with_only_included(tmp_path, monkeypatch):
    monkeypatch.setattr(flowmod, "BOAMP_PARTITIONS_DIR", tmp_path)
    # un avis de marché (gardé) et un avis d'attribution (filtré)
    monkeypatch.setattr(
        flowmod,
        "list_day_files",
        lambda d: ["legacy_mapa.xml", "eforms_award_notice.xml"],
    )

    def fake_download(d, name):
        return (FIXTURES / name).read_bytes()

    monkeypatch.setattr(flowmod, "download_xml", fake_download)

    part = process_day(date(2026, 1, 31))
    assert part.exists()
    df = pl.read_parquet(part)
    # seules les lignes du MAPA (appel d'offres) sont présentes
    assert df["source_format"].unique().to_list() == ["boamp"]
    assert df.height >= 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boamp_flow.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.flows.boamp_processing'`.

- [ ] **Step 3: Implement the flow**

Create `src/flows/boamp_processing.py`:

```python
"""Flow boamp_processing : acquisition + aplatissement + publication (issue #174)."""

from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from lxml import etree
from prefect import flow, task

from src.config import (
    BOAMP_BACKFILL_YEARS,
    BOAMP_DATASET_ID,
    BOAMP_PARTITIONS_DIR,
    BOAMP_RECRAWL_DAYS,
    BOAMP_START_DATE,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    LOG_LEVEL,
)
from src.tasks.boamp.fetch import (
    download_xml,
    iter_archive_xml,
    iter_days,
    list_day_files,
)
from src.tasks.boamp.flatten import annonce_to_rows, concat_partitions, rows_to_frame
from src.tasks.output import sink_to_files
from src.tasks.publish import publish_boamp_to_datagouv
from src.tasks.utils import get_logger, print_all_config


def _partition_path(d: date) -> Path:
    return BOAMP_PARTITIONS_DIR / f"{d.year}" / f"{d.month:02d}" / f"{d.day:02d}.parquet"


def _rows_from_bytes(xml_bytes: bytes, source_name: str) -> list[dict]:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return []
    return annonce_to_rows(root, source_name)


@task(retries=2, timeout_seconds=1800)
def process_day(d: date) -> Path:
    """Télécharge, aplatit et écrit la partition parquet d'un jour."""
    logger = get_logger(level=LOG_LEVEL)
    rows: list[dict] = []
    for filename in list_day_files(d):
        rows.extend(_rows_from_bytes(download_xml(d, filename), filename))

    part = _partition_path(d)
    part.parent.mkdir(parents=True, exist_ok=True)
    rows_to_frame(rows).write_parquet(part)
    logger.info(f"📦 {d.isoformat()} : {len(rows)} lignes -> {part.name}")
    return part


@task(retries=1, timeout_seconds=7200)
def backfill_year(year: int) -> Path:
    """Aplatit toutes les archives tar.gz d'une année dans un parquet annuel."""
    logger = get_logger(level=LOG_LEVEL)
    rows: list[dict] = []
    for name, xml_bytes in iter_archive_xml(year):
        rows.extend(_rows_from_bytes(xml_bytes, Path(name).name))
    out = BOAMP_PARTITIONS_DIR / f"backfill_{year}.parquet"
    rows_to_frame(rows).write_parquet(out)
    logger.info(f"🗄️ Backfill {year} : {len(rows)} lignes -> {out.name}")
    return out


@flow(log_prints=True)
def boamp_processing():
    logger = get_logger(level=LOG_LEVEL)
    logger.info("🚀  Début du flow boamp_processing")
    print_all_config()

    # 1. Backfill des années complètes (une fois, via marqueur).
    for year in BOAMP_BACKFILL_YEARS:
        marker = BOAMP_PARTITIONS_DIR / f".backfill_{year}.done"
        if marker.exists():
            continue
        backfill_year(year)
        marker.touch()

    # 2. Incrémental sur la période courante (crawl quotidien).
    # On ne crawle au jour le jour que ce qui n'est pas déjà couvert par le
    # backfill annuel : à partir du 1er janvier suivant la dernière année backfillée.
    crawl_start = date(max(BOAMP_BACKFILL_YEARS) + 1, 1, 1)
    start = max(date.fromisoformat(BOAMP_START_DATE), crawl_start)
    end = date.today()
    recrawl_floor = end - timedelta(days=BOAMP_RECRAWL_DAYS)
    for d in iter_days(start, end):
        part = _partition_path(d)
        if part.exists() and d < recrawl_floor:
            continue
        try:
            process_day(d)
        except Exception as e:  # un jour en échec ne doit pas tout arrêter
            logger.error(f"❌ Jour {d.isoformat()} en échec ({type(e).__name__}): {e}")

    # 3. Concaténation -> écriture -> publication.
    logger.info("Concaténation des partitions BOAMP...")
    lf = concat_partitions(BOAMP_PARTITIONS_DIR)
    sink_to_files(lf, DIST_DIR / "boamp")

    if DECP_PROCESSING_PUBLISH and BOAMP_DATASET_ID:
        logger.info("Publication BOAMP sur data.gouv.fr...")
        publish_boamp_to_datagouv()
    else:
        logger.info("Publication BOAMP désactivée (DECP_PROCESSING_PUBLISH/BOAMP_DATASET_ID).")

    logger.info("☑️  Fin du flow boamp_processing.")
```

Note: the `from lxml import etree` import is already required by `_rows_from_bytes`; ensure it is placed with the third-party imports (after `import polars as pl`).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boamp_flow.py -v`
Expected: PASS. (`process_day` runs as a plain function call in tests; Prefect `@task` is callable directly.)

- [ ] **Step 5: Commit**

```bash
git add src/flows/boamp_processing.py tests/test_boamp_flow.py
git commit -m "feat(boamp): flow boamp_processing (backfill + incrémental + publication) (#174)"
```

---

## Task 12: Wiring (publish, CLI, deployment) + end-to-end test

**Files:**

- Modify: `src/tasks/publish.py`
- Modify: `run_flow.py`
- Modify: `src/deployments.py`
- Test: `tests/test_boamp_flow.py`

- [ ] **Step 1: Add the publish helper**

Add to `src/tasks/publish.py` (import `BOAMP_*` from config at the top, then the function):

```python
from src.config import (
    BOAMP_CSV_RESOURCE_ID,
    BOAMP_DATASET_ID,
    BOAMP_PARQUET_RESOURCE_ID,
    DIST_DIR,
)  # merge into the existing config import


def publish_boamp_to_datagouv():
    """Met à jour les ressources parquet/CSV BOAMP sur data.gouv.fr."""
    logger = get_logger(level=LOG_LEVEL)
    uploads = [
        (DIST_DIR / "boamp.parquet", BOAMP_PARQUET_RESOURCE_ID),
        (DIST_DIR / "boamp.csv", BOAMP_CSV_RESOURCE_ID),
    ]
    for file_path, resource_id in uploads:
        if not resource_id:
            logger.warning(f"Resource id manquant pour {file_path.name}, ignoré.")
            continue
        logger.info(f"Mise à jour de {file_path.name}...")
        result = update_resource(
            BOAMP_DATASET_ID, resource_id, str(file_path), DATAGOUVFR_API_KEY
        )
        if result.get("success") is True:
            logger.info("OK")
```

- [ ] **Step 2: Register the flow in the CLI**

Modify `run_flow.py`:

```python
from src.flows.boamp_processing import boamp_processing
```

and add to the `FUNCTIONS` dict:

```python
    "boamp_processing": boamp_processing,
```

- [ ] **Step 3: Add the deployment**

Add to `src/deployments.py` inside the `if __name__ == "__main__":` block (mirror the existing `decp-processing` deployment; daily at 04:00):

```python
    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/boamp_processing.py:boamp_processing",
    ).deploy(
        name="boamp-processing",
        description="BOAMP/eForms : tous les jours à 4h00",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 4 * * *",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "DECP_DATA_DIR": "/home/yunohost.app/prefect",
            }
        },
    )
```

- [ ] **Step 4: Write the end-to-end test**

Append to `tests/test_boamp_flow.py`:

```python
def test_end_to_end_sink_from_fixtures(tmp_path, monkeypatch):
    """Aplatissement de toutes les fixtures -> parquet final cohérent."""
    from datetime import date

    monkeypatch.setattr(flowmod, "BOAMP_PARTITIONS_DIR", tmp_path / "parts")
    (tmp_path / "parts").mkdir()
    monkeypatch.setattr(
        flowmod,
        "list_day_files",
        lambda d: ["eforms_contract_notice.xml", "legacy_mapa.xml", "eforms_award_notice.xml"],
    )
    monkeypatch.setattr(flowmod, "download_xml", lambda d, name: (FIXTURES / name).read_bytes())

    process_day(date(2026, 1, 31))
    lf = flowmod.concat_partitions(tmp_path / "parts")
    out = tmp_path / "boamp"
    flowmod.sink_to_files(lf, out, file_format=["parquet"])

    df = pl.read_parquet(out.with_suffix(".parquet"))
    assert set(df["source_format"].unique().to_list()) == {"eforms", "boamp"}
    # l'avis d'attribution est exclu : pas de ligne ContractAwardNotice
    assert "ContractAwardNotice" not in df["type_document"].unique().to_list()
    # cœur présent
    for col in ("id_annonce", "objet", "lot_id", "acheteur_nom"):
        assert col in df.columns
```

- [ ] **Step 5: Run the full BOAMP test suite**

Run: `pytest tests/test_boamp_flatten.py tests/test_boamp_fetch.py tests/test_boamp_flow.py -v`
Expected: PASS (all).

- [ ] **Step 6: Verify CLI wiring and lint**

Run: `python -c "from run_flow import FUNCTIONS; print('boamp_processing' in FUNCTIONS)"`
Expected: `True`.

Run: `pre-commit run --all-files`
Expected: hooks pass (ruff/ruff-format/prettier/import-check), files possibly reformatted — re-add and re-run if so.

- [ ] **Step 7: Commit**

```bash
git add src/tasks/publish.py run_flow.py src/deployments.py tests/test_boamp_flow.py
git commit -m "feat(boamp): publication data.gouv, CLI run_flow, déploiement + test e2e (#174)"
```

---

## Post-implementation manual steps (not code)

These are prerequisites for production publication, to be done by the maintainer:

1. Créer le **dataset data.gouv.fr** BOAMP et deux ressources (parquet + CSV).
2. Renseigner `BOAMP_DATASET_ID`, `BOAMP_PARQUET_RESOURCE_ID`, `BOAMP_CSV_RESOURCE_ID` dans l'environnement de prod (et `.env`).
3. Premier run : le backfill 2024-2025 (tar.gz) peut être long ; le lancer une fois avec `python run_flow.py boamp_processing` avant d'activer le cron.

---

## Self-Review Notes (for the planner — verify during execution)

- **Spec coverage:** périmètre 2024+ (Task 1), filtrage NATURE commun + MISE*EN_CONCURRENCE (Task 3, 7), aplatissement générique + listes (Task 2, 8), grain lot + fallback (Task 4, 7), cœur harmonisé + préfixe ef*/bo\_ (Task 6), déréférencement acheteur + SIRET (Task 5), backfill tar.gz + incrémental + re-crawl (Task 9, 10, 11), sortie parquet/CSV (Task 8, 11), publication gardée (Task 12), tests dont exclusion attribution/préinfo et inclusion MISE_EN_CONCURRENCE (Task 7) — couverts.
- **Known limitations (documented, deferred to v2):**
  - attributs XML non capturés (texte des feuilles uniquement) ;
  - le **comptage par NATURE des avis ignorés** (suivi/observabilité évoqué dans la
    spec) n'est pas implémenté en v1 — `annonce_to_rows` renvoie `[]` sans statistique.
    À ajouter via un artefact Prefect si besoin de monitoring.
- **Type consistency:** `annonce_to_rows` → `rows_to_frame` → `concat_partitions` → `sink_to_files` ; `process_day`/`backfill_year` écrivent des parquets lus par `concat_partitions`. `extract_core` keys ⊂ `CORE_KEYS`. `publish_boamp_to_datagouv` lit `DIST_DIR/boamp.{parquet,csv}` écrits par `sink_to_files(lf, DIST_DIR/"boamp")`.

# BOAMP Field Definitions CSV — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Générer `reference/boamp_field_definitions.csv` qui, pour chaque colonne `List(String)` du parquet de sortie de `boamp_processing`, donne la définition de chaque élément XML parent (du plus général à la feuille) + la définition de la feuille seule.

**Architecture:** Logique pure dans `src/tasks/boamp/field_definitions.py` (testable via `from src.tasks.boamp.field_definitions import …`), CLI fin dans `script/build_boamp_field_definitions.py`. Deux régimes : legacy (dict nom→`<documentation>` depuis les XSD 3.2.5 FR) et eForms (chemin UBL normalisé → description FR du SDK eForms 1.10.5). Les fichiers SDK nécessaires sont vendorés dans `reference/eforms_sdk/1.10.5/`.

**Tech Stack:** Python 3.13, polars (lecture schéma parquet), lxml (parsing XSD + XML de traductions), json (fields.json), pytest.

**Issue:** #178

---

## File Structure

- `reference/eforms_sdk/1.10.5/fields.json` — copié depuis le SDK (vendoring, reproductibilité).
- `reference/eforms_sdk/1.10.5/field_fr.xml` — copié depuis le SDK.
- `reference/eforms_sdk/1.10.5/business-term_fr.xml` — copié depuis le SDK.
- `src/tasks/boamp/field_definitions.py` — **CRÉER** : fonctions pures (parsing, index, résolution, assemblage du DataFrame).
- `script/build_boamp_field_definitions.py` — **CRÉER** : CLI fin (chemins par défaut, écrit le CSV, affiche les stats de couverture).
- `tests/test_boamp_field_definitions.py` — **CRÉER** : tests des fonctions pures.
- `reference/boamp_field_definitions.csv` — **GÉNÉRÉ** par le script (livrable final).

---

## Task 1: Vendorer les fichiers SDK eForms

**Files:**

- Create: `reference/eforms_sdk/1.10.5/fields.json`
- Create: `reference/eforms_sdk/1.10.5/field_fr.xml`
- Create: `reference/eforms_sdk/1.10.5/business-term_fr.xml`

- [ ] **Step 1: Copier les 3 fichiers depuis le SDK kDrive**

```bash
SDK=/home/colin/kDrive/work/colmo/decp/eforms/eForms-SDK-1.10.5
mkdir -p reference/eforms_sdk/1.10.5
cp "$SDK/fields/fields.json" reference/eforms_sdk/1.10.5/fields.json
cp "$SDK/translations/field_fr.xml" reference/eforms_sdk/1.10.5/field_fr.xml
cp "$SDK/translations/business-term_fr.xml" reference/eforms_sdk/1.10.5/business-term_fr.xml
ls -la reference/eforms_sdk/1.10.5/
```

Expected: 3 fichiers présents (`fields.json` ~plusieurs Mo, les 2 xml ~centaines de Ko).

- [ ] **Step 2: Commit**

```bash
git add reference/eforms_sdk/1.10.5/
git commit -m "chore(boamp): vendorer fichiers SDK eForms 1.10.5 pour définitions champs (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `normalize_xpath`

Normalise un `xpathAbsolute` du SDK eForms en tuple de noms d'éléments locaux (sans préfixe namespace, sans prédicat `[...]`, sans pas d'attribut).

**Files:**

- Create: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py
from src.tasks.boamp.field_definitions import normalize_xpath


def test_normalize_xpath_simple():
    assert normalize_xpath("/*/cbc:RegulatoryDomain") == ("RegulatoryDomain",)


def test_normalize_xpath_strips_prefixes_and_predicates():
    xp = "/*/cac:TenderingTerms/cac:ProcurementLegislationDocumentReference[cbc:ID/text()='LocalLegalBasis']/cbc:DocumentDescription"
    assert normalize_xpath(xp) == (
        "TenderingTerms",
        "ProcurementLegislationDocumentReference",
        "DocumentDescription",
    )


def test_normalize_xpath_drops_attribute_steps():
    assert normalize_xpath("/*/cbc:NoticeTypeCode/@listName") == ("NoticeTypeCode",)
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py -v`
Expected: FAIL — `ImportError` / `cannot import name 'normalize_xpath'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py
"""Génère le CSV de définitions des colonnes du parquet BOAMP (#178).

Fonctions pures : parsing des XSD legacy et du SDK eForms, résolution des
chemins de colonnes vers des définitions humaines.
"""

import re

_PREDICATE = re.compile(r"\[[^\]]*\]")


def normalize_xpath(xpath: str) -> tuple[str, ...]:
    """Chemin xpathAbsolute eForms -> tuple de noms d'éléments locaux.

    Retire les prédicats ``[...]``, les préfixes de namespace (``cac:``…),
    les pas d'attribut (``@x``) et les ``/*`` racines.
    """
    xpath = _PREDICATE.sub("", xpath)
    segments: list[str] = []
    for step in xpath.split("/"):
        step = step.strip()
        if not step or step == "*" or step.startswith("@"):
            continue
        local = step.split(":")[-1]
        if "@" in local:
            continue
        segments.append(local)
    return tuple(segments)
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): normalize_xpath pour chemins eForms (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `build_legacy_index`

Construit `dict[nom_element -> définition_FR]` depuis les XSD legacy. Textes distincts pour un même nom joints par `/` ; les tirets de tête (`- Objet…`) sont retirés.

**Files:**

- Modify: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py (ajouter)
from pathlib import Path

from src.tasks.boamp.field_definitions import build_legacy_index

_MINI_XSD = """<?xml version="1.0"?>
<schema xmlns="http://www.w3.org/2001/XMLSchema">
  <element name="cp">
    <annotation><documentation>Code postal</documentation></annotation>
  </element>
  <element name="objet">
    <annotation><documentation>- Objet du marché</documentation></annotation>
  </element>
  <element name="objet">
    <annotation><documentation>Objet du marché</documentation></annotation>
  </element>
  <element name="sansdoc"/>
</schema>
"""


def test_build_legacy_index(tmp_path: Path):
    xsd = tmp_path / "mini.xsd"
    xsd.write_text(_MINI_XSD, encoding="utf-8")
    idx = build_legacy_index([xsd])
    assert idx["cp"] == "Code postal"
    # "- Objet du marché" et "Objet du marché" -> même texte après nettoyage -> dédoublonné
    assert idx["objet"] == "Objet du marché"
    assert "sansdoc" not in idx
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py::test_build_legacy_index -v`
Expected: FAIL — `cannot import name 'build_legacy_index'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py (ajouter en tête : imports)
from lxml import etree

_XS = "{http://www.w3.org/2001/XMLSchema}"
_WS = re.compile(r"\s+")


def _clean_text(text: str) -> str:
    """Compacte les espaces et retire un tiret/espace de tête décoratif."""
    return _WS.sub(" ", text).strip().lstrip("- ").strip()


def build_legacy_index(xsd_paths) -> dict[str, str]:
    """Mappe nom d'élément local -> définition FR depuis des XSD.

    Plusieurs textes DISTINCTS pour un même nom sont joints par ' / '.
    """
    texts: dict[str, set[str]] = {}
    for path in xsd_paths:
        tree = etree.parse(str(path))
        for el in tree.iter(f"{_XS}element"):
            name = el.get("name")
            if not name:
                continue
            doc = el.find(f"{_XS}annotation/{_XS}documentation")
            if doc is None or not doc.text:
                continue
            cleaned = _clean_text(doc.text)
            if cleaned:
                texts.setdefault(name, set()).add(cleaned)
    return {name: " / ".join(sorted(values)) for name, values in texts.items()}
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py::test_build_legacy_index -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): build_legacy_index depuis XSD 3.2.5 (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: `build_eforms_index`

Construit `dict[tuple_chemin_UBL -> description_FR]` depuis `fields.json` + `field_fr.xml`. Saute les champs d'attribut. Plusieurs descriptions pour un même chemin (prédicats fusionnés) jointes par `/`.

**Files:**

- Modify: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py (ajouter)
import json

from src.tasks.boamp.field_definitions import build_eforms_index

_FIELD_FR = """<?xml version="1.0" encoding="UTF-8"?>
<properties>
  <entry key="field|description|BT-01-notice">Base juridique de la procedure.</entry>
  <entry key="field|description|BT-02-notice">Type de l'avis.</entry>
  <entry key="field|name|BT-02-notice">Notice Type</entry>
</properties>
"""

_FIELDS_JSON = {
    "fields": [
        {"id": "BT-01-notice", "name": "Procedure Legal Basis",
         "xpathAbsolute": "/*/cbc:RegulatoryDomain"},
        {"id": "BT-02-notice", "name": "Notice Type",
         "xpathAbsolute": "/*/cbc:NoticeTypeCode"},
        {"id": "BT-03-notice", "name": "Form Type",
         "xpathAbsolute": "/*/cbc:NoticeTypeCode/@listName"},
        {"id": "BT-99-x", "name": "Sans FR",
         "xpathAbsolute": "/*/cac:Foo/cbc:Bar"},
    ]
}


def test_build_eforms_index(tmp_path):
    (tmp_path / "field_fr.xml").write_text(_FIELD_FR, encoding="utf-8")
    (tmp_path / "fields.json").write_text(json.dumps(_FIELDS_JSON), encoding="utf-8")
    idx = build_eforms_index(tmp_path)
    assert idx[("RegulatoryDomain",)] == "Base juridique de la procedure."
    assert idx[("NoticeTypeCode",)] == "Type de l'avis."
    # champ d'attribut ignoré : pas de clé ('NoticeTypeCode',) en doublon parasite,
    # et BT-03 (@listName) ne crée pas d'entrée propre
    # champ sans FR -> fallback sur le name EN
    assert idx[("Foo", "Bar")] == "Sans FR"
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py::test_build_eforms_index -v`
Expected: FAIL — `cannot import name 'build_eforms_index'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py (ajouter)
import json
from pathlib import Path


def _parse_translations(path, kind: str) -> dict[str, str]:
    """Fichier de traductions SDK -> {id -> texte} pour ``<kind>|description|<id>``."""
    out: dict[str, str] = {}
    tree = etree.parse(str(path))
    for entry in tree.iter("entry"):
        key = entry.get("key", "")
        parts = key.split("|")
        if len(parts) == 3 and parts[0] == kind and parts[1] == "description":
            text = (entry.text or "").strip()
            if text:
                out[parts[2]] = text
    return out


def build_eforms_index(sdk_dir) -> dict[tuple[str, ...], str]:
    """Mappe chemin UBL normalisé (relatif à /*) -> description FR.

    Source : ``fields.json`` (xpathAbsolute) + ``field_fr.xml`` (description FR),
    fallback sur le ``name`` anglais du field si pas de FR. Les champs d'attribut
    (xpath en ``/@x``) sont ignorés.
    """
    sdk_dir = Path(sdk_dir)
    field_fr = _parse_translations(sdk_dir / "field_fr.xml", "field")
    fields = json.loads(
        (sdk_dir / "fields.json").read_text(encoding="utf-8")
    )["fields"]

    paths: dict[tuple[str, ...], set[str]] = {}
    for field in fields:
        xpath = field.get("xpathAbsolute", "")
        if not xpath or "/@" in xpath or xpath.lstrip().startswith("@"):
            continue
        key = normalize_xpath(xpath)
        if not key:
            continue
        desc = field_fr.get(field["id"]) or field.get("name") or ""
        desc = desc.strip()
        if desc:
            paths.setdefault(key, set()).add(desc)
    return {key: " / ".join(sorted(values)) for key, values in paths.items()}
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py::test_build_eforms_index -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): build_eforms_index depuis SDK eForms (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Constantes et helpers de colonne

`STRUCTURAL`, `EFORMS_DOCTYPES`, `is_eforms`, `column_to_ubl_path`.

**Files:**

- Modify: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py (ajouter)
from src.tasks.boamp.field_definitions import (
    column_to_ubl_path,
    is_eforms,
)


def test_is_eforms():
    assert is_eforms("ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain")
    assert is_eforms("ProcurementProjectLot_TenderingTerms_FundingProgramCode")
    assert not is_eforms("ann_DONNEES_DSP_initial_objet")


def test_column_to_ubl_path_notice_level():
    col = "ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain"
    assert column_to_ubl_path(col) == ("RegulatoryDomain",)


def test_column_to_ubl_path_lot_level():
    col = "ProcurementProjectLot_TenderingTerms_FundingProgramCode"
    assert column_to_ubl_path(col) == (
        "ProcurementProjectLot",
        "TenderingTerms",
        "FundingProgramCode",
    )
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py::test_is_eforms -v`
Expected: FAIL — `cannot import name 'is_eforms'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py (ajouter)
EFORMS_DOCTYPES = frozenset({
    "ContractNotice",
    "ContractAwardNotice",
    "PriorInformationNotice",
    "BusinessRegistrationInformationNotice",
})

# Segments purement structurels, omis de la colonne `definitions`.
STRUCTURAL = frozenset({
    "ann",
    "DONNEES",
    "EFORMS",
    "UBLExtensions",
    "UBLExtension",
    "ExtensionContent",
    "EformsExtension",
}) | EFORMS_DOCTYPES


def is_eforms(col: str) -> bool:
    return "EFORMS" in col or col.startswith("ProcurementProjectLot")


def column_to_ubl_path(col: str) -> tuple[str, ...]:
    """Nom de colonne eForms -> chemin UBL relatif à /* (pour matcher l'index).

    - ``ann_DONNEES_EFORMS_<DocType>_…`` -> segments après <DocType> ;
    - ``ProcurementProjectLot_…`` -> tels quels (xpaths SDK sous /*/cac:ProcurementProjectLot).
    Conserve les wrappers (UBLExtensions…) car ils font partie des xpaths du SDK.
    """
    parts = col.split("_")
    if parts[:3] == ["ann", "DONNEES", "EFORMS"]:
        rest = parts[3:]
        if rest and rest[0] in EFORMS_DOCTYPES:
            rest = rest[1:]
        return tuple(rest)
    return tuple(parts)
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py -k "is_eforms or column_to_ubl_path" -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): constantes structurelles + helpers de colonne (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: `resolve_column`

Découpe une colonne, omet les segments structurels, résout chaque segment restant et formate `(definitions, definition_feuille)`.

**Files:**

- Modify: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py (ajouter)
from src.tasks.boamp.field_definitions import resolve_column

_NOT_FOUND = "(définition introuvable)"


def test_resolve_column_legacy():
    legacy = {"DSP": "Délégation de service public", "adr": "Informations d'adresse",
              "cp": "Code postal"}
    eforms = {}
    col = "ann_DONNEES_DSP_adr_cp"
    defs, leaf = resolve_column(col, legacy, eforms)
    # 'ann' et 'DONNEES' omis (structurels)
    assert defs == (
        "DSP : Délégation de service public\n"
        "adr : Informations d'adresse\n"
        "cp : Code postal"
    )
    assert leaf == "Code postal"


def test_resolve_column_legacy_missing_segment():
    legacy = {"cp": "Code postal"}
    defs, leaf = resolve_column("ann_DONNEES_DSP_cp", legacy, {})
    assert defs == f"DSP : {_NOT_FOUND}\ncp : Code postal"
    assert leaf == "Code postal"


def test_resolve_column_eforms_leaf_resolved():
    eforms = {("RegulatoryDomain",): "Base juridique de la procedure."}
    col = "ann_DONNEES_EFORMS_ContractNotice_RegulatoryDomain"
    defs, leaf = resolve_column(col, {}, eforms)
    # seuls les segments non-structurels restent : RegulatoryDomain (la feuille)
    assert defs == "RegulatoryDomain : Base juridique de la procedure."
    assert leaf == "Base juridique de la procedure."


def test_resolve_column_eforms_intermediate_best_effort():
    eforms = {
        (
            "UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension",
            "Organizations", "Organization", "Company", "PostalAddress", "CityName",
        ): "Ville de l'organisation.",
    }
    col = ("ann_DONNEES_EFORMS_ContractNotice_UBLExtensions_UBLExtension_"
           "ExtensionContent_EformsExtension_Organizations_Organization_Company_"
           "PostalAddress_CityName")
    defs, leaf = resolve_column(col, {}, eforms)
    # wrappers structurels omis ; intermédiaires non-feuille -> introuvable ; feuille OK
    assert defs == (
        f"Organizations : {_NOT_FOUND}\n"
        f"Organization : {_NOT_FOUND}\n"
        f"Company : {_NOT_FOUND}\n"
        f"PostalAddress : {_NOT_FOUND}\n"
        "CityName : Ville de l'organisation."
    )
    assert leaf == "Ville de l'organisation."
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py -k resolve_column -v`
Expected: FAIL — `cannot import name 'resolve_column'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py (ajouter)
_NOT_FOUND = "(définition introuvable)"


def resolve_column(
    col: str,
    legacy_index: dict[str, str],
    eforms_index: dict[tuple[str, ...], str],
) -> tuple[str, str]:
    """(definitions multi-lignes, definition_feuille) pour une colonne.

    Les segments structurels sont omis. eForms : la feuille est résolue par match
    du chemin UBL complet ; les segments intermédiaires sont best-effort
    (souvent introuvables). Legacy : chaque segment via le dict par nom.
    """
    meaningful = [s for s in col.split("_") if s not in STRUCTURAL]
    if not meaningful:
        return "", ""

    if is_eforms(col):
        leaf_def = eforms_index.get(column_to_ubl_path(col), "")
        lines = []
        for seg in meaningful:
            if seg is meaningful[-1] and leaf_def:
                lines.append(f"{seg} : {leaf_def}")
            else:
                lines.append(f"{seg} : {_NOT_FOUND}")
        return "\n".join(lines), (leaf_def or _NOT_FOUND)

    lines = [f"{seg} : {legacy_index.get(seg, _NOT_FOUND)}" for seg in meaningful]
    leaf_def = legacy_index.get(meaningful[-1], _NOT_FOUND)
    return "\n".join(lines), leaf_def
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py -k resolve_column -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): resolve_column (legacy + eForms best-effort) (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: `load_list_columns` + `build_definitions_frame`

Lit les colonnes `List(String)` du parquet et assemble le DataFrame final (trié par nom de colonne).

**Files:**

- Modify: `src/tasks/boamp/field_definitions.py`
- Test: `tests/test_boamp_field_definitions.py`

- [ ] **Step 1: Écrire le test qui échoue**

```python
# tests/test_boamp_field_definitions.py (ajouter)
import polars as pl

from src.tasks.boamp.field_definitions import (
    build_definitions_frame,
    load_list_columns,
)


def test_load_list_columns(tmp_path):
    p = tmp_path / "mini.parquet"
    pl.DataFrame({
        "id_annonce": ["x"],          # String -> exclu
        "ann_DONNEES_DSP_cp": [["75001"]],  # List(String) -> gardé
    }).write_parquet(p)
    cols = load_list_columns(p)
    assert cols == ["ann_DONNEES_DSP_cp"]


def test_build_definitions_frame(tmp_path):
    p = tmp_path / "mini.parquet"
    pl.DataFrame({
        "id_annonce": ["x"],
        "ann_DONNEES_DSP_cp": [["75001"]],
    }).write_parquet(p)
    legacy = {"DSP": "Délégation de service public", "cp": "Code postal"}
    df = build_definitions_frame(p, legacy_index=legacy, eforms_index={})
    assert df.columns == ["nom_colonne", "definitions", "definition_feuille"]
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["nom_colonne"] == "ann_DONNEES_DSP_cp"
    assert row["definitions"] == (
        "DSP : Délégation de service public\ncp : Code postal"
    )
    assert row["definition_feuille"] == "Code postal"
```

- [ ] **Step 2: Lancer le test, vérifier l'échec**

Run: `pytest tests/test_boamp_field_definitions.py -k "load_list_columns or build_definitions_frame" -v`
Expected: FAIL — `cannot import name 'load_list_columns'`.

- [ ] **Step 3: Implémenter**

```python
# src/tasks/boamp/field_definitions.py (ajouter en tête : import polars)
import polars as pl


def load_list_columns(parquet_path) -> list[str]:
    """Noms des colonnes de type List(String) du parquet (schéma seul)."""
    schema = pl.scan_parquet(parquet_path).collect_schema()
    return [name for name in schema.names() if isinstance(schema[name], pl.List)]


def build_definitions_frame(
    parquet_path,
    legacy_index: dict[str, str],
    eforms_index: dict[tuple[str, ...], str],
) -> pl.DataFrame:
    """DataFrame {nom_colonne, definitions, definition_feuille}, trié par nom."""
    rows = []
    for col in sorted(load_list_columns(parquet_path)):
        definitions, leaf = resolve_column(col, legacy_index, eforms_index)
        rows.append({
            "nom_colonne": col,
            "definitions": definitions,
            "definition_feuille": leaf,
        })
    return pl.DataFrame(
        rows,
        schema=["nom_colonne", "definitions", "definition_feuille"],
        orient="row",
    )
```

- [ ] **Step 4: Lancer le test, vérifier le succès**

Run: `pytest tests/test_boamp_field_definitions.py -k "load_list_columns or build_definitions_frame" -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py
git commit -m "feat(boamp): assemblage du DataFrame de définitions (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: CLI `script/build_boamp_field_definitions.py`

Chemins par défaut, écrit le CSV, affiche les stats de couverture.

**Files:**

- Create: `script/build_boamp_field_definitions.py`

- [ ] **Step 1: Implémenter le CLI**

```python
# script/build_boamp_field_definitions.py
"""Génère reference/boamp_field_definitions.csv (#178).

Usage : python script/build_boamp_field_definitions.py
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.tasks.boamp.field_definitions import (  # noqa: E402
    build_definitions_frame,
    build_eforms_index,
    build_legacy_index,
)

_LEGACY_XSD_DIR = (
    REPO
    / "reference/boamp_schemas/jo_boamp_opendata_v3.2.5.1/3.2.5/sdk1.9/main"
)
LEGACY_XSDS = [
    _LEGACY_XSD_DIR / "jo_boamp.xsd",
    _LEGACY_XSD_DIR / "jo_types.xsd",
    _LEGACY_XSD_DIR / "jo_boamp_opendata.xsd",
]
SDK_DIR = REPO / "reference/eforms_sdk/1.10.5"
PARQUET = REPO / "dist/boamp.parquet"
OUTPUT = REPO / "reference/boamp_field_definitions.csv"

_NOT_FOUND = "(définition introuvable)"


def main() -> None:
    legacy = build_legacy_index(LEGACY_XSDS)
    eforms = build_eforms_index(SDK_DIR)
    df = build_definitions_frame(PARQUET, legacy_index=legacy, eforms_index=eforms)
    df.write_csv(OUTPUT)

    total = df.height
    leaves_found = df.filter(
        df["definition_feuille"] != _NOT_FOUND
    ).height
    print(f"✅ {OUTPUT.relative_to(REPO)} : {total} colonnes")
    print(f"   feuilles définies : {leaves_found}/{total} "
          f"({100 * leaves_found // max(total, 1)} %)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Lancer le script**

Run: `python script/build_boamp_field_definitions.py`
Expected: affiche `✅ reference/boamp_field_definitions.csv : 999 colonnes` + le taux de feuilles définies (attendu : legacy ~100 %, eForms partiel → global élevé mais < 100 %).

- [ ] **Step 3: Vérification de cohérence du CSV**

Run:

```bash
python3 -c "import polars as pl; df=pl.read_csv('reference/boamp_field_definitions.csv'); print(df.shape); print(df.filter(pl.col('nom_colonne')=='ann_GESTION_REFERENCE_IDWEB').row(0, named=True))"
```

Expected: shape `(999, 3)` ; la ligne IDWEB a une `definition_feuille` non vide (élément GESTION documenté).

- [ ] **Step 4: Lint**

Run: `pre-commit run --files script/build_boamp_field_definitions.py src/tasks/boamp/field_definitions.py tests/test_boamp_field_definitions.py`
Expected: ruff/format/check_imports PASS (corriger si besoin).

- [ ] **Step 5: Commit**

```bash
git add script/build_boamp_field_definitions.py reference/boamp_field_definitions.csv
git commit -m "feat(boamp): CLI build_boamp_field_definitions + CSV généré (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: Revue de couverture eForms et itération

Examiner les colonnes eForms dont la feuille est `(définition introuvable)` et améliorer le matching (préfixe lot, doc types) si le gain est simple.

**Files:**

- Modify (si besoin): `src/tasks/boamp/field_definitions.py`

- [ ] **Step 1: Lister les feuilles eForms non résolues**

Run:

```bash
python3 -c "
import polars as pl
df = pl.read_csv('reference/boamp_field_definitions.csv')
miss = df.filter((pl.col('definition_feuille')=='(définition introuvable)') & (pl.col('nom_colonne').str.contains('EFORMS') | pl.col('nom_colonne').str.starts_with('ProcurementProjectLot')))
print('non résolues eForms:', miss.height)
print(miss['nom_colonne'].head(30).to_list())
"
```

Expected: liste les colonnes à diagnostiquer (attendu : surtout `ProcurementProjectLot_*` + champs à prédicats).

- [ ] **Step 2: Décider**

Si une amélioration simple et générale ressort (ex. un préfixe lot mal normalisé), l'implémenter sous TDD (ajouter un test ciblé dans `tests/test_boamp_field_definitions.py` reproduisant un vrai chemin non résolu, puis corriger). Sinon, laisser tel quel : eForms est best-effort par design (voir spec). **Ne pas** sur-ingénierer.

- [ ] **Step 3: Régénérer + commit (si modifié)**

```bash
python script/build_boamp_field_definitions.py
git add -A
git commit -m "fix(boamp): améliore matching eForms des définitions (#178)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes d'implémentation

- **Encodage** : tous les fichiers sont UTF-8 ; les XSD legacy peuvent contenir des accents — `etree.parse` gère l'encodage déclaré.
- **`pl.List`** : le test `isinstance(schema[name], pl.List)` couvre `List(String)` (seul type liste produit par `flatten`).
- **Newlines dans le CSV** : `pl.DataFrame.write_csv` quote automatiquement les cellules contenant `\n` — la colonne `definitions` reste sur plusieurs lignes dans la cellule.
- **Réexécution** : le script est idempotent et régénérable (`python script/build_boamp_field_definitions.py`).

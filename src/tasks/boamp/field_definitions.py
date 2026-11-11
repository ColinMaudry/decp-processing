"""Génère le CSV de définitions des colonnes du parquet BOAMP (#178).

Fonctions pures : parsing des XSD legacy et du SDK eForms, résolution des
chemins de colonnes vers des définitions humaines.
"""

import json
import re
from pathlib import Path

import polars as pl
from lxml import etree

_PREDICATE = re.compile(r"\[[^\]]*\]")
_XS = "{http://www.w3.org/2001/XMLSchema}"
_WS = re.compile(r"\s+")


def _clean_text(text: str) -> str:
    """Compacte les espaces et retire un tiret/espace de tête décoratif."""
    return _WS.sub(" ", text).strip().lstrip("- ").strip()


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


def build_legacy_index(xsd_paths: list) -> dict[str, str]:
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
    fields = json.loads((sdk_dir / "fields.json").read_text(encoding="utf-8"))["fields"]

    paths: dict[tuple[str, ...], set[str]] = {}
    for field in fields:
        xpath = field.get("xpathAbsolute", "")
        if not xpath:
            continue
        # Strip predicates before checking for attribute steps: a /@ inside
        # [...] is part of a predicate condition, not an attribute-axis step.
        xpath_no_pred = _PREDICATE.sub("", xpath)
        if "/@" in xpath_no_pred or xpath_no_pred.lstrip().startswith("@"):
            continue
        key = normalize_xpath(xpath)
        if not key:
            continue
        desc = field_fr.get(field["id"]) or field.get("name") or ""
        desc = desc.strip()
        if desc:
            paths.setdefault(key, set()).add(desc)
    return {key: " / ".join(sorted(values)) for key, values in paths.items()}


EFORMS_DOCTYPES = frozenset(
    {
        "ContractNotice",
        "ContractAwardNotice",
        "PriorInformationNotice",
        "BusinessRegistrationInformationNotice",
    }
)

# Segments purement structurels, omis de la colonne `definitions`.
STRUCTURAL = (
    frozenset(
        {
            "ann",
            "DONNEES",
            "EFORMS",
            "UBLExtensions",
            "UBLExtension",
            "ExtensionContent",
            "EformsExtension",
        }
    )
    | EFORMS_DOCTYPES
)


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
        rows.append(
            {
                "nom_colonne": col,
                "definitions": definitions,
                "definition_feuille": leaf,
            }
        )
    return pl.DataFrame(
        rows,
        schema=["nom_colonne", "definitions", "definition_feuille"],
        orient="row",
    )

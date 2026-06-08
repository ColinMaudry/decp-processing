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
    REPO / "reference/boamp_schemas/jo_boamp_opendata_v3.2.5.1/3.2.5/sdk1.9/main"
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
    leaves_found = df.filter(df["definition_feuille"] != _NOT_FOUND).height
    print(f"✅ {OUTPUT.relative_to(REPO)} : {total} colonnes")
    print(
        f"   feuilles définies : {leaves_found}/{total} "
        f"({100 * leaves_found // max(total, 1)} %)"
    )


if __name__ == "__main__":
    main()

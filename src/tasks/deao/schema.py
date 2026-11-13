"""Schéma des appels d'offres (DEAO) : contrat avis-lot des adaptateurs et schéma publié."""

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from src.config import REFERENCE_DIR

# Description des colonnes publiées, au format de reference/schema_base.json (DECP) :
# l'ordre du fichier est celui du parquet publié.
SCHEMA_DEAO_PATH = REFERENCE_DIR / "schema_deao.json"

S = pl.String
DATE = pl.Date

# Type Table Schema → type polars des colonnes publiées.
_DTYPES = {
    "string": pl.String,
    "number": pl.Float64,
    "integer": pl.Int64,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime("us", "Europe/Paris"),
    "array": pl.List(pl.String),
}


@dataclass(frozen=True)
class Field:
    name: str
    dtype: pl.DataType
    title: str
    description: str
    short_title: str | None = None
    enum: tuple[str, ...] | None = None
    closed_only: bool = False


def _charger_schema() -> list[dict]:
    with open(SCHEMA_DEAO_PATH, encoding="utf-8") as f:
        return json.load(f)["fields"]


PUBLISHED_FIELDS: tuple[Field, ...] = tuple(
    Field(
        name=champ["name"],
        dtype=_DTYPES[champ["type"]],
        title=champ["title"],
        description=champ["description"],
        short_title=champ.get("short_title"),
        enum=tuple(champ["enum"]) if "enum" in champ else None,
        closed_only=champ.get("closed_only", False),
    )
    for champ in _charger_schema()
)

_DESCRIPTIVES = (
    "referenceAcheteur",
    "acheteur_id",
    "acheteur_nom",
    "objet",
    "lotId",
    "lotIntitule",
    "description",
    "montantEstime",
    "type",
    "codeCPV",
    "procedure",
    "techniques",
    "dureeMois",
    "dateLimiteReponse",
    "lieuExecution_code",
    "lieuExecution_typeCode",
    "urlConsultation",
    "urlAvis",
    "sourceDataset",
    "sourceFile",
)

_PUBLISHED_DTYPES = {f.name: f.dtype for f in PUBLISHED_FIELDS}

AVIS_LOT_SCHEMA: dict[str, pl.DataType] = {
    **{name: _PUBLISHED_DTYPES[name] for name in _DESCRIPTIVES},
    "idAvis": S,
    "avisPrecedent": S,
    "typeAvis": S,
    "preInformation": pl.Boolean,
    "avisComplet": pl.Boolean,
    "datePublicationAvis": DATE,
    "infosRectificatif": S,
    "formatSource": S,
    "acheteur_adresse_voie": S,
    "acheteur_adresse_cp": S,
    "acheteur_adresse_ville": S,
    "acheteur_adresse_nuts": S,
    "acheteur_pays": S,
    "idEforms": S,
    "idEformsPrecedent": S,
}


def published_schema() -> dict[str, pl.DataType]:
    return dict(_PUBLISHED_DTYPES)


def selection_publiee(df: pl.DataFrame) -> pl.DataFrame:
    """Colonnes du schéma publié, dans son ordre et à ses types (null si absentes)."""
    return df.select(
        pl.col(f.name).cast(f.dtype)
        if f.name in df.columns
        else pl.lit(None, dtype=f.dtype).alias(f.name)
        for f in PUBLISHED_FIELDS
    )


def open_schema() -> dict[str, pl.DataType]:
    """Champs publiés en Open Data : le schéma publié sans les champs `closed_only`."""
    return {f.name: f.dtype for f in PUBLISHED_FIELDS if not f.closed_only}


def conform_avis(df: pl.DataFrame) -> pl.DataFrame:
    """Complète (null), caste et ordonne un DataFrame selon AVIS_LOT_SCHEMA."""
    inconnues = [c for c in df.columns if c not in AVIS_LOT_SCHEMA]
    if inconnues:
        raise ValueError(f"Colonnes inconnues du contrat avis-lot : {inconnues}")
    return df.select(
        [
            pl.col(name).cast(dtype)
            if name in df.columns
            else pl.lit(None, dtype=dtype).alias(name)
            for name, dtype in AVIS_LOT_SCHEMA.items()
        ]
    )


def empty_avis_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=AVIS_LOT_SCHEMA)


def write_table_schema(path: Path) -> Path:
    """Écrit le Table Schema JSON du schéma Open Data (reference/schema_deao.json sans les
    champs `closed_only`, mise en forme comme le schema.json de DECP)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"fields": [c for c in _charger_schema() if not c.get("closed_only")]},
            f,
            indent=4,
            ensure_ascii=False,
        )
    return path

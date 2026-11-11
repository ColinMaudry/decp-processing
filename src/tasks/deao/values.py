import re
from datetime import datetime, time
from functools import cache
from math import ceil
from zoneinfo import ZoneInfo

import polars as pl

from src.config import BASE_DIR

PARIS = ZoneInfo("Europe/Paris")
NUTS3_PATH = BASE_DIR / "reference" / "nuts3_departements.csv"


def normalize_lot_id(raw: str | None) -> str | None:
    """'LOT-0001' -> '1', 'PAR-0001' -> '1', '03' -> '3', 'A' -> 'A'."""
    if raw is None:
        return None
    raw = str(raw).strip()
    if not raw:
        return None
    match = re.fullmatch(r"(?:[A-Za-z]+-)?(\d+)", raw)
    if match:
        return str(int(match.group(1)))
    return raw


def duree_mois(valeur: str | float | None, unite: str) -> int | None:
    """Convertit une durée en mois entiers (arrondi supérieur)."""
    if valeur is None:
        return None
    try:
        n = float(valeur)
    except (TypeError, ValueError):
        return None
    unite = (unite or "").strip()
    if unite in ("MONTH", "mois"):
        return ceil(n)
    if unite in ("DAY", "jours"):
        return ceil(n / 30)
    if unite in ("YEAR", "ans"):
        return ceil(n * 12)
    return None


_DATE_OFFSET = re.compile(r"(\d{4}-\d{2}-\d{2})(Z|[+-]\d{2}:\d{2})?")


def _parse_time(heure: str) -> time | None:
    try:
        return datetime.fromisoformat(
            f"2000-01-01T{heure.replace('Z', '+00:00')}"
        ).time()
    except ValueError:
        return None


def parse_datetime_paris(
    valeur: str | None, heure: str | None = None
) -> datetime | None:
    """Datetime aware Europe/Paris ; naïf = heure de Paris ; date seule = 23:59:59."""
    if not valeur:
        return None
    valeur = valeur.strip()
    try:
        if _DATE_OFFSET.fullmatch(valeur):
            jour = datetime.fromisoformat(valeur[:10]).date()
            offset = valeur[10:]
            if heure:
                dt = datetime.fromisoformat(
                    f"{jour}T{heure.strip().replace('Z', '+00:00')}"
                )
                if dt.tzinfo is None and offset:
                    dt = datetime.fromisoformat(
                        f"{jour}T{heure.strip()}{offset.replace('Z', '+00:00')}"
                    )
            else:
                dt = datetime.fromisoformat(
                    f"{jour}T23:59:59{offset.replace('Z', '+00:00')}"
                )
        else:
            dt = datetime.fromisoformat(valeur.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=PARIS)
    return dt.astimezone(PARIS)


def departement_depuis_cp(cp: str | None) -> str | None:
    if not cp:
        return None
    cp = str(cp).strip()
    if not re.fullmatch(r"\d{5}", cp):
        return None
    if cp[:2] in ("97", "98"):
        return cp[:3]
    if cp[:2] == "20":
        return "2A" if int(cp[:3]) <= 201 else "2B"
    return cp[:2]


@cache
def _nuts3_departements() -> dict[str, str]:
    df = pl.read_csv(NUTS3_PATH, schema_overrides={"departement_code": pl.String})
    return dict(zip(df["nuts3"], df["departement_code"], strict=True))


def departement_depuis_nuts(nuts: str | None) -> str | None:
    if not nuts:
        return None
    return _nuts3_departements().get(nuts.strip().upper())

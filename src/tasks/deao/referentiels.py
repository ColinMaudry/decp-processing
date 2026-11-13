from collections import Counter
from collections.abc import Callable
from datetime import date
from pathlib import Path

import httpx
import polars as pl
from tenacity import RetryError

from src.config import LOG_LEVEL
from src.tasks.deao import geocode_cache
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA
from src.tasks.utils import get_logger


def charger_referentiels(
    sirene_dir: Path, data_dir: Path
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    """Unités légales et établissements SIRENE (préparés par sirene_preprocess), COG."""
    chemins = [
        sirene_dir / "unites_legales.parquet",
        sirene_dir / "etablissements.parquet",
        data_dir / "code_officiel_geographique.parquet",
    ]
    manquants = [str(p) for p in chemins if not p.exists()]
    if manquants:
        raise FileNotFoundError(
            "Référentiels absents (lancer sirene_preprocess / get_cog) : "
            + ", ".join(manquants)
        )
    ul, etab, cog = chemins
    return pl.scan_parquet(ul), pl.scan_parquet(etab), pl.read_parquet(cog)


def geocoder_tolerant(
    today: date,
    stats: Counter,
    geocoder_adresses: Callable[[pl.DataFrame, date], pl.DataFrame] | None = None,
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """Géocodeur qui, en cas de panne de la Géoplateforme, n'arrête pas le run :
    aucune adresse géocodée (les replis département s'appliquent)."""
    geocoder_adresses = geocoder_adresses or geocode_cache.geocoder_adresses

    def geocoder(adresses: pl.DataFrame) -> pl.DataFrame:
        try:
            return geocoder_adresses(adresses, today)
        except (httpx.HTTPError, RetryError) as e:
            stats["geocodage_en_erreur"] += 1
            get_logger(level=LOG_LEVEL).warning(
                f"Géocodage indisponible, poursuite sans géocodage : {e!r}"
            )
            return pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA)

    return geocoder

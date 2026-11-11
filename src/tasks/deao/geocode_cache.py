from collections.abc import Callable
from datetime import date, timedelta

import polars as pl

from src.config import DEAO_DATA_DIR, DEAO_GEOCODE_S3_PREFIX, GEOCODING_RETRY_DAYS
from src.tasks.geocode import build_address_geocoding_csv, geocode_csv
from src.tasks.get import get_from_s3
from src.tasks.publish import publish_to_s3

CACHE_FILENAME = "adresse_geocode.parquet"

ADRESSE_GEOCODE_SCHEMA = {
    "adresse_cle": pl.String,
    "commune_code": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "score": pl.Float64,
    "status": pl.String,
    "geocoded_at": pl.Date,
}


def _normalise(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.to_lowercase().str.replace_all(r"\s+", " ")


def adresse_cle_expr() -> pl.Expr:
    cp = pl.col("acheteur_adresse_cp").str.replace_all(r"\s+", "")
    return (
        pl.when(cp.is_null() & pl.col("acheteur_adresse_ville").is_null())
        .then(None)
        .otherwise(
            pl.concat_str(
                [
                    _normalise("acheteur_adresse_voie").fill_null(""),
                    cp.fill_null(""),
                    _normalise("acheteur_adresse_ville").fill_null(""),
                ],
                separator="|",
            )
        )
        .alias("adresse_cle")
    )


def a_geocoder(
    cles: pl.DataFrame, cache: pl.DataFrame, today: date, retry_days: int
) -> pl.DataFrame:
    limite = today - timedelta(days=retry_days)
    a_refaire = cache.filter(
        (pl.col("status") == "failed") & (pl.col("geocoded_at") <= limite)
    )["adresse_cle"]
    connues = cache.filter(~pl.col("adresse_cle").is_in(a_refaire))["adresse_cle"]
    return cles.filter(~pl.col("adresse_cle").is_in(connues))


def _geocode_adresses(adresses: pl.DataFrame) -> pl.DataFrame:
    return geocode_csv(
        adresses,
        key_column="adresse_cle",
        with_citycode=True,
        csv_builder=build_address_geocoding_csv,
        empty_schema=ADRESSE_GEOCODE_SCHEMA,
    )


def geocoder_adresses(
    adresses: pl.DataFrame,
    today: date,
    geocode: Callable[[pl.DataFrame], pl.DataFrame] = _geocode_adresses,
) -> pl.DataFrame:
    adresses = adresses.filter(pl.col("adresse_cle").is_not_null()).unique(
        subset="adresse_cle", keep="first", maintain_order=True
    )
    cache_lf = get_from_s3(CACHE_FILENAME, prefix=DEAO_GEOCODE_S3_PREFIX)
    cache = (
        pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA)
        if cache_lf is None
        else cache_lf.select(list(ADRESSE_GEOCODE_SCHEMA)).collect()
    )
    delta = a_geocoder(adresses, cache, today, GEOCODING_RETRY_DAYS)
    if delta.height == 0:
        return cache

    nouveaux = geocode(delta).select(list(ADRESSE_GEOCODE_SCHEMA))
    cache = pl.concat(
        [
            cache.join(nouveaux.select("adresse_cle"), on="adresse_cle", how="anti"),
            nouveaux,
        ]
    ).sort("adresse_cle")
    path = DEAO_DATA_DIR / CACHE_FILENAME
    cache.write_parquet(path)
    publish_to_s3(path, prefix=DEAO_GEOCODE_S3_PREFIX)
    return cache

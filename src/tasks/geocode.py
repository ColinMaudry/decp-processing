import io
from datetime import date

import httpx
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import (
    GEOCODING_API_URL,
    GEOCODING_CHUNK_SIZE,
    GEOCODING_MIN_SCORE,
    SIRET_LATLONG_SCHEMA,
    logger,
)


def build_geocoding_csv(addresses: pl.DataFrame) -> bytes:
    df = addresses.select(
        pl.col("siret"),
        pl.concat_str(
            [
                pl.col("numeroVoieEtablissement").fill_null(""),
                pl.col("indiceRepetitionEtablissement").fill_null(""),
                pl.col("typeVoieEtablissement").fill_null(""),
                pl.col("libelleVoieEtablissement").fill_null(""),
            ],
            separator=" ",
        )
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("q"),
        pl.col("codePostalEtablissement").cast(pl.String).alias("postcode"),
        pl.col("commune_code").cast(pl.String).alias("citycode"),
    )
    buf = io.BytesIO()
    df.write_csv(buf)
    return buf.getvalue()


def parse_geocoding_results(
    csv_bytes: bytes,
    min_score: float,
    today: date,
) -> pl.DataFrame:
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        null_values=["[ND]"],
        schema_overrides={
            "siret": pl.String,
            "result_score": pl.Float64,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
        },
        columns=["siret", "result_score", "latitude", "longitude"],
    )
    is_success = pl.col("result_score").is_not_null() & (
        pl.col("result_score") >= min_score
    )
    return df.select(
        pl.col("siret"),
        pl.when(is_success).then(pl.col("latitude")).otherwise(None).alias("latitude"),
        pl.when(is_success)
        .then(pl.col("longitude"))
        .otherwise(None)
        .alias("longitude"),
        pl.lit("geoplateforme").alias("source"),
        pl.col("result_score").alias("score"),
        pl.lit(today).alias("geocoded_at"),
        pl.when(is_success)
        .then(pl.lit("success"))
        .otherwise(pl.lit("failed"))
        .alias("status"),
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=20))
def post_geocoding_chunk(csv_bytes: bytes) -> bytes:
    with httpx.Client(timeout=120.0) as client:
        response = client.post(
            f"{GEOCODING_API_URL}/search/csv/",
            files={"data": ("input.csv", csv_bytes, "text/csv")},
            data={
                "columns": "q",
                "postcode": "postcode",
                "citycode": "citycode",
            },
        )
        response.raise_for_status()
        return response.content


def geocode_csv(
    addresses: pl.DataFrame,
    chunk_size: int = GEOCODING_CHUNK_SIZE,
    min_score: float = GEOCODING_MIN_SCORE,
) -> pl.DataFrame:
    logger.info("Géocodage via Géoplateforme...")
    today = date.today()
    results = []
    for i in range(0, addresses.height, chunk_size):
        chunk = addresses.slice(i, chunk_size)
        csv_bytes = build_geocoding_csv(chunk)
        response_csv = post_geocoding_chunk(csv_bytes)
        results.append(parse_geocoding_results(response_csv, min_score, today))
    if not results:
        return pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)
    return pl.concat(results)

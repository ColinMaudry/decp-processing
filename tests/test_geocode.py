import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.geocode import pad_siret_latlong_schema


def test_pad_siret_latlong_adds_missing_columns():
    legacy = pl.LazyFrame(
        {
            "siret": ["12345678901234"],
            "latitude": [48.85],
            "longitude": [2.35],
        }
    )
    padded = pad_siret_latlong_schema(legacy)
    df = padded.collect()
    assert set(df.columns) == set(SIRET_LATLONG_SCHEMA.keys())
    assert df["source"].to_list() == ["decp"]
    assert df["status"].to_list() == ["success"]
    assert df["score"].is_null().all()
    assert df["geocoded_at"].is_null().all()


def test_pad_siret_latlong_preserves_existing_columns():
    current = pl.LazyFrame(
        {
            "siret": ["12345678901234"],
            "latitude": [48.85],
            "longitude": [2.35],
            "source": ["geoplateforme"],
            "score": [0.92],
            "geocoded_at": [None],
            "status": ["success"],
        },
        schema_overrides={"geocoded_at": pl.Date},
    )
    padded = pad_siret_latlong_schema(current).collect()
    assert padded["source"].to_list() == ["geoplateforme"]
    assert padded["score"].to_list() == [0.92]

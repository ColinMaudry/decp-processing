import io

import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.geocode import (
    build_geocoding_csv,
    geocode_csv,
    pad_siret_latlong_schema,
    parse_geocoding_results,
)


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


# --- build_geocoding_csv ---


def test_build_geocoding_csv_concatenates_address():
    addresses = pl.DataFrame(
        {
            "siret": ["12345678901234"],
            "numeroVoieEtablissement": ["10"],
            "indiceRepetitionEtablissement": ["bis"],
            "typeVoieEtablissement": ["RUE"],
            "libelleVoieEtablissement": ["DE LA PAIX"],
            "codePostalEtablissement": ["75001"],
            "codeCommuneEtablissement": ["75101"],
        }
    )
    csv_bytes = build_geocoding_csv(addresses)
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        schema_overrides={
            "siret": pl.String,
            "postcode": pl.String,
            "citycode": pl.String,
        },
    )
    assert df.columns == ["siret", "q", "postcode", "citycode"]
    assert df["q"].to_list() == ["10 bis RUE DE LA PAIX"]
    assert df["postcode"].to_list() == ["75001"]
    assert df["citycode"].to_list() == ["75101"]


def test_build_geocoding_csv_handles_null_address_fields():
    addresses = pl.DataFrame(
        {
            "siret": ["12345678901234"],
            "numeroVoieEtablissement": [None],
            "indiceRepetitionEtablissement": [None],
            "typeVoieEtablissement": ["AVENUE"],
            "libelleVoieEtablissement": ["DES CHAMPS"],
            "codePostalEtablissement": ["75008"],
            "codeCommuneEtablissement": ["75108"],
        }
    )
    df = pl.read_csv(
        io.BytesIO(build_geocoding_csv(addresses)),
        schema_overrides={
            "siret": pl.String,
            "postcode": pl.String,
            "citycode": pl.String,
        },
    )
    assert df["q"].to_list() == ["AVENUE DES CHAMPS"]


# --- parse_geocoding_results ---


def test_parse_results_marks_high_score_as_success():
    from datetime import date

    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
    ).encode()
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "success"
    assert row["latitude"] == 48.869
    assert row["longitude"] == 2.331
    assert row["score"] == 0.92
    assert row["source"] == "geoplateforme"
    from datetime import date as date_cls

    assert row["geocoded_at"] == date_cls(2026, 5, 15)


def test_parse_results_marks_low_score_as_failed():
    from datetime import date

    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,FOO BAR,75001,75101,0.2,48.0,2.0\n"
    ).encode()
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "failed"
    assert row["latitude"] is None
    assert row["longitude"] is None
    assert row["score"] == 0.2


def test_parse_results_handles_no_match():
    from datetime import date

    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,FOO BAR,75001,75101,,,\n"
    ).encode()
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "failed"
    assert row["latitude"] is None


# --- geocode_csv ---


def test_geocode_csv_calls_api_and_returns_dataframe(monkeypatch):
    import httpx

    _original_Client = httpx.Client

    addresses = pl.DataFrame(
        {
            "siret": ["12345678901234"],
            "numeroVoieEtablissement": ["10"],
            "indiceRepetitionEtablissement": [None],
            "typeVoieEtablissement": ["RUE"],
            "libelleVoieEtablissement": ["DE LA PAIX"],
            "codePostalEtablissement": ["75001"],
            "codeCommuneEtablissement": ["75101"],
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/search/csv/")
        body = (
            "siret,q,postcode,citycode,result_score,latitude,longitude\n"
            "12345678901234,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
        )
        return httpx.Response(200, content=body)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: _original_Client(
            transport=transport,
            **{k: v for k, v in kw.items() if k != "transport"},
        ),
    )

    result = geocode_csv(addresses)
    assert result.height == 1
    assert result.row(0, named=True)["status"] == "success"


def test_geocode_csv_chunks_large_input(monkeypatch):
    import httpx

    _original_Client = httpx.Client

    n = 12
    addresses = pl.DataFrame(
        {
            "siret": [f"{i:014d}" for i in range(n)],
            "numeroVoieEtablissement": ["1"] * n,
            "indiceRepetitionEtablissement": [None] * n,
            "typeVoieEtablissement": ["RUE"] * n,
            "libelleVoieEtablissement": ["FOO"] * n,
            "codePostalEtablissement": ["75001"] * n,
            "codeCommuneEtablissement": ["75101"] * n,
        }
    )
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        body_in = request.read()
        assert b"siret" in body_in
        return httpx.Response(
            200,
            content=b"siret,q,postcode,citycode,result_score,latitude,longitude\n",
        )

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: _original_Client(
            transport=transport,
            **{k: v for k, v in kw.items() if k != "transport"},
        ),
    )

    geocode_csv(addresses, chunk_size=5)
    assert call_count["n"] == 3

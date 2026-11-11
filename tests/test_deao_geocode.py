from datetime import date

import polars as pl

from src.tasks.deao import geocode_cache
from src.tasks.deao.geocode_cache import (
    ADRESSE_GEOCODE_SCHEMA,
    a_geocoder,
    adresse_cle_expr,
    geocoder_adresses,
)
from src.tasks.geocode import build_address_geocoding_csv, parse_geocoding_results

TODAY = date(2026, 11, 11)


def test_parse_geocoding_results_citycode():
    csv = b"adresse_cle,result_score,latitude,longitude,result_citycode\nk1,0.9,48.8,2.3,75104\nk2,0.2,1,1,99999\n"
    df = parse_geocoding_results(
        csv, 0.5, TODAY, key_column="adresse_cle", with_citycode=True
    )
    assert df.filter(pl.col("adresse_cle") == "k1")["commune_code"][0] == "75104"
    assert df.filter(pl.col("adresse_cle") == "k2")["commune_code"][0] is None
    assert df.filter(pl.col("adresse_cle") == "k2")["status"][0] == "failed"


def test_build_address_geocoding_csv():
    df = pl.DataFrame({"adresse_cle": ["k"], "q": ["1 rue x"], "postcode": ["29260"]})
    out = build_address_geocoding_csv(df).decode()
    assert out.splitlines()[0] == "adresse_cle,q,postcode,citycode"
    assert out.splitlines()[1] in ("k,1 rue x,29260,", 'k,1 rue x,29260,""')


def _cache(rows):
    return pl.DataFrame(
        {
            "adresse_cle": [r[0] for r in rows],
            "commune_code": ["1"] * len(rows),
            "latitude": [1.0] * len(rows),
            "longitude": [1.0] * len(rows),
            "score": [0.9] * len(rows),
            "status": [r[1] for r in rows],
            "geocoded_at": [r[2] for r in rows],
        },
        schema=ADRESSE_GEOCODE_SCHEMA,
    )


def test_a_geocoder_retry():
    cache = _cache(
        [
            ("ok", "success", date(2026, 1, 1)),
            ("vieux_echec", "failed", date(2026, 10, 1)),
            ("echec_recent", "failed", date(2026, 11, 10)),
        ]
    )
    cles = pl.DataFrame(
        {"adresse_cle": ["ok", "vieux_echec", "echec_recent", "nouvelle"]}
    )
    res = a_geocoder(cles, cache, TODAY, 30)
    assert set(res["adresse_cle"]) == {"vieux_echec", "nouvelle"}


def test_adresse_cle_expr():
    df = pl.DataFrame(
        {
            "acheteur_adresse_voie": [" 12 Bvd  X ", None, None],
            "acheteur_adresse_cp": ["29260", " 29 260", None],
            "acheteur_adresse_ville": ["Lesneven", None, None],
        }
    ).select(adresse_cle_expr())
    assert df["adresse_cle"].to_list() == ["12 bvd x|29260|lesneven", "|29260|", None]


def _patch_s3(monkeypatch, cache, tmp_path):
    published = []
    monkeypatch.setattr(
        geocode_cache,
        "get_from_s3",
        lambda key, prefix="": None if cache is None else cache.lazy(),
    )
    monkeypatch.setattr(
        geocode_cache,
        "publish_to_s3",
        lambda p, prefix="": published.append((p, prefix)),
    )
    monkeypatch.setattr(geocode_cache, "DEAO_DATA_DIR", tmp_path)
    return published


def test_geocoder_adresses_ne_republie_pas_sans_delta(monkeypatch, tmp_path):
    cache = _cache([("a", "success", date(2026, 11, 1))])
    published = _patch_s3(monkeypatch, cache, tmp_path)

    def boom(_):
        raise AssertionError("ne doit pas géocoder")

    adresses = pl.DataFrame(
        {"adresse_cle": ["a", None], "q": ["x", "y"], "postcode": ["1", "2"]}
    )
    res = geocoder_adresses(adresses, TODAY, geocode=boom)
    assert published == []
    assert res["adresse_cle"].to_list() == ["a"]


def test_geocoder_adresses_fusionne_et_publie(monkeypatch, tmp_path):
    cache = _cache([("a", "failed", date(2026, 1, 1)), ("z", "success", TODAY)])
    published = _patch_s3(monkeypatch, cache, tmp_path)
    seen = []

    def fake(delta):
        seen.append(delta["adresse_cle"].to_list())
        return pl.DataFrame(
            {
                "adresse_cle": ["a", "b"],
                "commune_code": ["29103", "75104"],
                "latitude": [1.0, 2.0],
                "longitude": [1.0, 2.0],
                "score": [0.9, 0.8],
                "status": ["success", "success"],
                "geocoded_at": [TODAY, TODAY],
            }
        )

    adresses = pl.DataFrame(
        {
            "adresse_cle": ["a", "a", "b"],
            "q": ["x", "x", "y"],
            "postcode": ["1", "1", "2"],
        }
    )
    res = geocoder_adresses(adresses, TODAY, geocode=fake)
    assert seen == [["a", "b"]]
    assert res["adresse_cle"].to_list() == ["a", "b", "z"]
    assert res["status"].to_list() == ["success"] * 3
    assert res["commune_code"][0] == "29103"
    assert (
        published == [(tmp_path / "adresse_geocode.parquet", "deao")]
        or len(published) == 1
    )
    assert (tmp_path / "adresse_geocode.parquet").exists()


def test_geocoder_adresses_sans_cache(monkeypatch, tmp_path):
    published = _patch_s3(monkeypatch, None, tmp_path)
    res = geocoder_adresses(
        pl.DataFrame(
            {"adresse_cle": [], "q": [], "postcode": []},
            schema={"adresse_cle": pl.String, "q": pl.String, "postcode": pl.String},
        ),
        TODAY,
        geocode=lambda d: pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA),
    )
    assert res.height == 0 and published == []

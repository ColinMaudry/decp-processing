import io
import threading
import time
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from botocore.exceptions import ClientError

from src.tasks.deao import geocode_cache
from src.tasks.deao.geocode_cache import (
    ADRESSE_GEOCODE_SCHEMA,
    a_geocoder,
    adresse_cle_expr,
    geocoder_adresses,
)
from src.tasks.geocode import build_address_geocoding_csv, parse_geocoding_results
from tests.deao_fakes import FakeS3

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


def _patch_s3(monkeypatch, cache, tmp_path, fake=None):
    fake = fake or FakeS3()
    if cache is not None:
        buf = io.BytesIO()
        cache.write_parquet(buf)
        fake.objects["deao/adresse_geocode.parquet"] = buf.getvalue()
    published = []
    monkeypatch.setattr(geocode_cache, "s3_client", lambda: fake)
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


def test_geocoder_adresses_cle_absente_geocode_tout(monkeypatch, tmp_path):
    published = _patch_s3(monkeypatch, None, tmp_path)
    seen = []

    def fake(delta):
        seen.append(delta["adresse_cle"].to_list())
        return pl.DataFrame(
            {
                "adresse_cle": ["a"],
                "commune_code": ["29103"],
                "latitude": [1.0],
                "longitude": [1.0],
                "score": [0.9],
                "status": ["success"],
                "geocoded_at": [TODAY],
            }
        )

    adresses = pl.DataFrame({"adresse_cle": ["a"], "q": ["x"], "postcode": ["1"]})
    res = geocoder_adresses(adresses, TODAY, geocode=fake)
    assert seen == [["a"]]
    assert res.height == 1 and len(published) == 1


@pytest.mark.parametrize("code", ["AccessDenied", "InternalError"])
def test_geocoder_adresses_erreur_s3_remonte(monkeypatch, tmp_path, code):
    class Broken(FakeS3):
        def get_object(self, Bucket, Key, **kwargs):
            raise ClientError({"Error": {"Code": code, "Message": "x"}}, "GetObject")

    published = _patch_s3(monkeypatch, None, tmp_path, fake=Broken())
    adresses = pl.DataFrame({"adresse_cle": ["a"], "q": ["x"], "postcode": ["1"]})
    with pytest.raises(ClientError):
        geocoder_adresses(adresses, TODAY, geocode=lambda d: d)
    assert published == []


def _geocode_lent(delta):
    time.sleep(0.2)
    return pl.DataFrame(
        {
            "adresse_cle": delta["adresse_cle"],
            "commune_code": ["29103"] * delta.height,
            "latitude": [1.0] * delta.height,
            "longitude": [1.0] * delta.height,
            "score": [0.9] * delta.height,
            "status": ["success"] * delta.height,
            "geocoded_at": [TODAY] * delta.height,
        }
    )


def test_geocoder_adresses_concurrents_sans_perte(monkeypatch, tmp_path):
    # Deux plateformes géocodent en parallèle : aucune mise à jour du cache perdue.
    fake = FakeS3()
    _patch_s3(monkeypatch, None, tmp_path, fake=fake)
    televerses = []

    def publier(p, prefix=""):
        televerses.append(Path(p).name)
        fake.objects[f"{prefix}/{Path(p).name}"] = Path(p).read_bytes()

    monkeypatch.setattr(geocode_cache, "publish_to_s3", publier)
    fils = [
        threading.Thread(
            target=geocoder_adresses,
            args=(pl.DataFrame({"adresse_cle": [cle]}), TODAY, _geocode_lent),
        )
        for cle in ("a", "b")
    ]
    for f in fils:
        f.start()
    for f in fils:
        f.join()
    final = pl.read_parquet(io.BytesIO(fake.objects["deao/adresse_geocode.parquet"]))
    assert final["adresse_cle"].to_list() == ["a", "b"]
    # Le fichier téléversé garde le nom de la clé S3.
    assert televerses == ["adresse_geocode.parquet"] * 2
    assert (tmp_path / "adresse_geocode.parquet").exists()

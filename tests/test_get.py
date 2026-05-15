import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import bootstrap_siret_latlong, get_etablissements

REQUIRED_GEO_COLUMNS = {
    "numeroVoieEtablissement",
    "indiceRepetitionEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
}


def test_get_etablissements_includes_geocoding_columns():
    lf = get_etablissements()
    assert isinstance(lf, pl.LazyFrame)
    columns = set(lf.collect_schema().names())
    missing = REQUIRED_GEO_COLUMNS - columns
    assert not missing, f"Colonnes manquantes : {missing}"


def test_bootstrap_siret_latlong_produces_extended_schema(tmp_path, monkeypatch):
    fake_decp = tmp_path / "decp_fake.parquet"
    pl.DataFrame(
        {
            "acheteur_id": ["12345678901234", None, "00000000000000"],
            "acheteur_latitude": [48.85, None, 0.0],
            "acheteur_longitude": [2.35, None, 0.0],
            "titulaire_id": ["98765432109876", "98765432109876", None],
            "titulaire_latitude": [45.75, 45.75, None],
            "titulaire_longitude": [4.85, 4.85, None],
        }
    ).write_parquet(fake_decp)

    _original_scan = pl.scan_parquet

    def fake_scan(url, **kwargs):
        if str(url).startswith("https://"):
            return _original_scan(fake_decp)
        return _original_scan(url, **kwargs)

    monkeypatch.setattr("src.tasks.get.pl.scan_parquet", fake_scan)
    monkeypatch.setattr("src.tasks.get.publish_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("src.tasks.get.DATA_DIR", tmp_path)

    lf = bootstrap_siret_latlong()
    df = lf.collect()

    assert set(df.columns) == set(SIRET_LATLONG_SCHEMA.keys())
    assert df["source"].unique().to_list() == ["decp"]
    assert df["status"].unique().to_list() == ["success"]
    assert df["score"].is_null().all()
    assert df["geocoded_at"].is_null().all()
    assert df["siret"].n_unique() == df.height

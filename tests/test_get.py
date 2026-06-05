import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    xml_stream_to_parquet,
)

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


def test_xml_stream_to_parquet_small_file_is_not_empty(tmp_path):
    """Régression : un petit XML (ndjson < taille du buffer d'écriture) doit
    quand même produire des lignes. Sans flush() avant scan_ndjson, le buffer
    n'était jamais vidé sur le disque et le parquet ressortait vide (0 ligne)."""
    xml = (
        '<?xml version="1.0" encoding="ISO-8859-15"?>\n'
        "<marches>\n"
        " <marche>\n"
        "  <id>20261115446200</id>\n"
        "  <acheteur><id>21130112200084</id></acheteur>\n"
        "  <nature>March\xe9</nature>\n"
        "  <montant>262578.28</montant>\n"
        "  <dateNotification>2026-02-11</dateNotification>\n"
        " </marche>\n"
        "</marches>\n"
    ).encode("ISO-8859-15")
    xml_path = tmp_path / "decp_small.xml"
    xml_path.write_bytes(xml)

    output_path = tmp_path / "out"
    xml_stream_to_parquet(str(xml_path), output_path, fix_chars=False)

    df = pl.read_parquet(output_path.with_suffix(".parquet"))
    assert df.height == 1
    assert df["acheteur_id"].to_list() == ["21130112200084"]
    # L'ISO-8859-15 doit être décodé correctement
    assert df["nature"].to_list() == ["Marché"]


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

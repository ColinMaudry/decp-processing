import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    json_stream_to_parquet,
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


def test_json_stream_to_parquet_small_file_detects_format(tmp_path):
    """Régression : un petit JSON DECP 2019 (< chunk_size) doit être détecté même si
    le premier chunk est tronqué par le buffering de stream_replace_bytestring (BOM
    + NaN->null), qui garde en réserve les derniers octets au cas où un motif serait
    coupé entre deux chunks. Avant le fix, la détection de format n'essayait que ce
    premier chunk tronqué et abandonnait avec le warning "Pas de match trouvé"."""
    content = (
        '{"$schema":"https://raw.githubusercontent.com/etalab/format-commande-publique'
        '/master/sch%C3%A9mas/json/paquet.json","marches":[{"id":"2019031700",'
        '"acheteur":{"id":"20005340300057","nom":"Région Normandie"},'
        '"nature":"Marché","objet":"Maintenance et assistance du logiciel CINDOC",'
        '"codeCPV":"72267100","procedure":"Marché négocié sans '
        'publicité ni mise en concurrence préalable","lieuExecution":'
        '{"code":"28000","typeCode":"Code postal","nom":"REGION NORMANDIE"},'
        '"dureeMois":48,"dateNotification":"2019-09-09",'
        '"datePublicationDonnees":"2019-10-04","montant":200000,'
        '"formePrix":"Révisable","titulaires":[{"typeIdentifiant":"SIRET",'
        '"id":"44882586900028","denominationSociale":"TECHNODOC"}],'
        '"modifications":[],"_type":"Marché"}]}'
    ).encode("utf-8")

    json_path = tmp_path / "decp_small.json"
    json_path.write_bytes(content)

    output_path = tmp_path / "out"
    resource = {
        "dataset_code": "decp_minef",
        "ori_filename": "decp_small.json",
        "dataset_name": "test",
    }

    fields, decp_format = json_stream_to_parquet(str(json_path), output_path, resource)

    assert decp_format is not None
    assert decp_format.label == "DECP 2019"
    df = pl.read_parquet(output_path.with_suffix(".parquet"))
    assert df.height == 1
    assert df["id"].to_list() == ["2019031700"]


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

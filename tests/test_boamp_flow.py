from datetime import date
from pathlib import Path

import polars as pl

import src.flows.boamp_processing as flowmod
from src.flows.boamp_processing import process_day
from src.tasks.boamp.flatten import concat_partitions
from src.tasks.output import sink_to_files

FIXTURES = Path(__file__).parent / "data" / "boamp"


def test_process_day_writes_partition_with_only_included(tmp_path, monkeypatch):
    monkeypatch.setattr(flowmod, "BOAMP_PARTITIONS_DIR", tmp_path)
    # un avis de marché (gardé) et un avis d'attribution (filtré)
    monkeypatch.setattr(
        flowmod,
        "list_day_files",
        lambda d: ["legacy_mapa.xml", "eforms_award_notice.xml"],
    )

    def fake_download(d, name):
        return (FIXTURES / name).read_bytes()

    monkeypatch.setattr(flowmod, "download_xml", fake_download)

    part = process_day(date(2026, 1, 31))
    assert part.exists()
    df = pl.read_parquet(part)
    # seules les lignes du MAPA (appel d'offres) sont présentes
    assert df["source_format"].unique().to_list() == ["boamp"]
    assert df.height >= 1


def test_end_to_end_sink_from_fixtures(tmp_path, monkeypatch):
    """Aplatissement de toutes les fixtures -> parquet final cohérent."""
    monkeypatch.setattr(flowmod, "BOAMP_PARTITIONS_DIR", tmp_path / "parts")
    (tmp_path / "parts").mkdir()
    monkeypatch.setattr(
        flowmod,
        "list_day_files",
        lambda d: [
            "eforms_contract_notice.xml",
            "legacy_mapa.xml",
            "eforms_award_notice.xml",
        ],
    )
    monkeypatch.setattr(
        flowmod, "download_xml", lambda d, name: (FIXTURES / name).read_bytes()
    )

    process_day(date(2026, 1, 31))
    lf = concat_partitions(tmp_path / "parts")
    out = tmp_path / "boamp"
    sink_to_files(lf, out, file_format=["parquet"])

    df = pl.read_parquet(out.with_suffix(".parquet"))
    assert set(df["source_format"].unique().to_list()) == {"eforms", "boamp"}
    # l'avis d'attribution est exclu : pas de ligne ContractAwardNotice
    assert "ContractAwardNotice" not in df["type_document"].unique().to_list()
    # cœur présent
    for col in ("id_annonce", "objet", "lot_id", "acheteur_nom"):
        assert col in df.columns

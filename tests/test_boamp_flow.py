from datetime import date
from pathlib import Path

import polars as pl

import src.flows.boamp_processing as flowmod
from src.flows.boamp_processing import process_day

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

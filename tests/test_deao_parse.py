from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.tasks.deao.schema import AVIS_LOT_SCHEMA
from src.tasks.deao.sources.boamp.parse import parse_day
from src.tasks.deao.values import PARIS

FIXTURES = Path(__file__).parent / "data" / "deao"


def test_parse_day_fixtures():
    files = {p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")}
    df, c = parse_day(files)
    assert c["lu"] == 13 and c["retenu"] == 11
    assert c["exclu:MODIFICATION"] == 1 and c["exclu:ATTRIBUTION"] == 1
    assert df.schema == pl.Schema(AVIS_LOT_SCHEMA)
    r = df.filter(pl.col("idAvis") == "26-99074").row(0, named=True)
    assert (r["avisPrecedent"], r["typeAvis"], r["avisComplet"]) == (
        "26-87868",
        "rectificatif",
        True,
    )
    assert r["sourceDataset"] == "boamp" and r["formatSource"] == "eforms"
    # la date limite tz-aware ne glisse pas (rectificatif : 30/10)
    limite = r["dateLimiteReponse"]
    assert limite is not None
    assert limite.tzinfo is not None
    assert limite == limite.astimezone(PARIS)
    assert (limite.day, limite.month) == (30, 10)
    r = df.filter(pl.col("idAvis") == "26-99891").row(0, named=True)
    assert r["avisComplet"] is False and r["datePublicationAvis"] == date(2026, 10, 14)
    assert r["sourceFile"].endswith("/2026/10/14/26-99891.xml")


def test_parse_day_datetime_aller_retour():
    files = {"26-99074.xml": (FIXTURES / "26-99074.xml").read_bytes()}
    df, _ = parse_day(files)
    valeurs = df["dateLimiteReponse"].drop_nulls().to_list()
    assert valeurs
    assert all(isinstance(v, datetime) and v.tzinfo is not None for v in valeurs)


def test_parse_day_xml_illisible():
    files = {
        "26-1.xml": b"<pas du xml",
        "26-99870.xml": (FIXTURES / "26-99870.xml").read_bytes(),
    }
    df, c = parse_day(files)
    assert c["erreur_xml"] == 1 and df.height == 1


def test_parse_day_vide():
    df, c = parse_day({})
    assert df.height == 0 and df.schema == pl.Schema(AVIS_LOT_SCHEMA)

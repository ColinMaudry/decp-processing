import re
from datetime import date, datetime
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from src.config import BOAMP_BASE_URL
from src.tasks.deao.schema import AVIS_LOT_SCHEMA
from src.tasks.deao.sources.boamp.parse import parse_day, source_file_expr
from src.tasks.deao.sources.boamp.xml import idweb
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


def test_parse_day_isole_un_avis_dont_le_mapping_echoue(monkeypatch):
    from src.tasks.deao.sources.boamp import parse

    vrai_map_legacy = parse.map_legacy

    def map_legacy(root, classement, compteur):
        if idweb(root) == "26-99870":
            raise KeyError("DONNEES")
        return vrai_map_legacy(root, classement, compteur)

    monkeypatch.setattr(parse, "map_legacy", map_legacy)
    files = {p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")}
    df, c = parse_day(files)
    assert c["erreur_mapping"] == 1
    assert c["retenu"] == 10
    assert "26-99870" not in df["idAvis"].to_list()
    assert df.filter(pl.col("idAvis") == "26-99891").height == 1


def test_parse_day_compte_les_avis_sans_date_publication():
    xml = (FIXTURES / "26-99870.xml").read_bytes()
    xml_sans_date = re.sub(
        rb"<DATE_PUBLICATION>[^<]*</DATE_PUBLICATION>",
        b"<DATE_PUBLICATION>inconnue</DATE_PUBLICATION>",
        xml,
    )
    assert xml_sans_date != xml
    df, c = parse_day({"26-99870.xml": xml_sans_date})
    assert c["sans_date_publication"] == 1
    assert df["sourceFile"].to_list() == [None]


def test_source_file_expr_annees_passees_sous_flux_historique():
    df = pl.DataFrame(
        {
            "sourceFile": [
                f"{BOAMP_BASE_URL}/2025/11/05/25-1.xml",
                f"{BOAMP_BASE_URL}/2026/10/14/26-1.xml",
                f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/2024/03/12/24-1.xml",
                None,
            ]
        }
    )
    out = df.select(source_file_expr(2026))["sourceFile"].to_list()
    assert out == [
        f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/2025/11/05/25-1.xml",
        f"{BOAMP_BASE_URL}/2026/10/14/26-1.xml",
        f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/2024/03/12/24-1.xml",
        None,
    ]
    # l'année suivante, 2026 bascule à son tour
    assert df.select(source_file_expr(2027))["sourceFile"][1] == (
        f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/2026/10/14/26-1.xml"
    )


def test_parse_day_id_eforms_et_acheteur_pays():
    files = {p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")}
    df, _ = parse_day(files)
    r = df.filter(pl.col("idAvis") == "26-87868").row(0, named=True)
    assert r["idEforms"] == "17114689-cb66-44e1-b420-be7e501a33b0"
    legacy = df.filter(pl.col("idAvis") == "26-92574").row(0, named=True)
    assert legacy["idEforms"] is None and legacy["idEformsPrecedent"] is None
    assert set(df["acheteur_pays"].to_list()) == {"FRA"}


def test_parse_day_inchange_hors_nouvelles_colonnes():
    """Régression de la refonte : sortie figée (parse_day_expected.parquet)."""
    files = {p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")}
    df, _ = parse_day(files)
    attendu = pl.read_parquet(FIXTURES / "parse_day_expected.parquet")
    nouvelles = ["idEforms", "idEformsPrecedent", "acheteur_pays"]
    assert df.drop(nouvelles).columns == attendu.columns
    assert_frame_equal(df.drop(nouvelles), attendu)

from collections import Counter
from pathlib import Path

import polars as pl
import pytest

from src.tasks.deao.sources.joue import identifiants
from src.tasks.deao.sources.joue.identifiants import (
    charger_cache_identifiants,
    resoudre_avec_cache,
    resoudre_avis_precedents,
)
from src.tasks.deao.sources.joue.parse import parse_day
from tests.deao_fakes import fake_s3, fake_ted, no_sleep  # noqa: F401

DATA = Path(__file__).parent / "data" / "deao" / "joue"
UUID = "c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"


def _avis(*numeros):
    files = {f"{n}.xml": (DATA / f"{n}.xml").read_bytes() for n in numeros}
    df, _ = parse_day(files)
    return df


def _prec(df, id_avis):
    return df.filter(pl.col("idAvis") == id_avis)["avisPrecedent"].unique().to_list()[0]


@pytest.fixture
def s3_identifiants(fake_s3, monkeypatch, tmp_path):  # noqa: F811
    monkeypatch.setattr(identifiants, "s3_client", lambda: fake_s3)
    monkeypatch.setattr(identifiants, "DEAO_DATA_DIR", tmp_path)

    def publier(path, prefix=""):
        fake_s3.objects[f"{prefix.strip('/')}/{path.name}"] = path.read_bytes()

    monkeypatch.setattr(identifiants, "publish_to_s3", publier)
    return fake_s3


def test_resolution_interne():
    avis, stats = _avis("677195-2026", "706679-2026"), Counter()
    out = resoudre_avis_precedents(
        avis, stats, resolver=lambda ids: pytest.fail("pas d'API")
    )
    assert _prec(out, "706679-2026") == "677195-2026"
    assert stats["joue:identifiant_interne"] == 1
    assert out.height == avis.height


def test_resolution_externe():
    avis, stats = _avis("706679-2026"), Counter()
    out = resoudre_avis_precedents(
        avis, stats, resolver=lambda ids: {UUID: "677195-2026"}
    )
    assert _prec(out, "706679-2026") == "677195-2026"
    assert stats["joue:identifiant_resolu"] == 1


def test_echec_racine_elle_meme():
    avis, stats = _avis("706679-2026"), Counter()
    out = resoudre_avis_precedents(avis, stats, resolver=lambda ids: {})
    assert _prec(out, "706679-2026") is None
    assert stats["joue:identifiant_non_resolu"] == 1


def test_cache_ne_garde_que_les_succes(s3_identifiants, fake_ted):  # noqa: F811
    resoudre_avec_cache([UUID, "00000000-0000-0000-0000-000000000000"])
    assert charger_cache_identifiants().height == 1
    fake_ted.appels.clear()
    assert resoudre_avec_cache([UUID]) == {UUID: "677195-2026"}
    assert fake_ted.appels == []


def test_api_en_erreur_non_cachee(s3_identifiants, fake_ted, no_sleep):  # noqa: F811
    fake_ted.identifiants_en_erreur.add(UUID)
    assert resoudre_avec_cache([UUID]) == {}
    assert charger_cache_identifiants().height == 0


def test_lignes_boamp_et_joue_sans_precedent_inchangees():
    from tests.test_deao_consolidate import avis, frame

    df = frame(
        [
            avis(idAvis="B2", avisPrecedent="B1"),
            avis(sourceDataset="joue", idAvis="J1", idEforms="e-1"),
        ]
    )
    out = resoudre_avis_precedents(
        df, Counter(), resolver=lambda ids: pytest.fail("pas d'API")
    )
    assert out.equals(df)

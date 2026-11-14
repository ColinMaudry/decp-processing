# ruff: noqa: F811
import dataclasses
import io
from types import SimpleNamespace

import polars as pl
import pytest

from src import config
from src.flows.deao_processing import deao_processing
from src.flows.scrap_open import selection_publiee
from src.tasks.deao.report import lignes_rapport_sources
from src.tasks.deao.schema import open_schema, published_schema
from tests.deao_fakes import fake_datagouv  # noqa: F401

OPEN = "ds-a"
PARQUET_A, CSV_A, SCHEMA_A = "res-a-parquet", "res-a-csv", "res-a-schema"
PARQUET_B = "b86dd1f4-68f5-4847-9a25-c6d234aa3d66"


def _source_parquet(code: str, n: int = 3, uids: list[str] | None = None) -> bytes:
    uids = uids or [f"{code}-{i}" for i in range(n)]
    df = pl.DataFrame(
        {
            "uid": uids,
            "sourceDataset": [code] * len(uids),
            "statut": ["ouvert"] * len(uids),
            "objet": ["travaux"] * len(uids),
            "acheteur_id_source": ["siret"] * len(uids),
        }
    )
    buf = io.BytesIO()
    selection_publiee(df).write_parquet(buf)
    return buf.getvalue()


@pytest.fixture
def env(monkeypatch, tmp_path, fake_datagouv):
    registre = config.DEAO_REGISTRE
    assert {s.code for s in registre} == {"boamp", "joue"}
    dist = tmp_path / "dist"
    monkeypatch.setattr(config, "DEAO_DIST_DIR", dist)
    monkeypatch.setattr(config, "DEAO_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", "true")
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    monkeypatch.setattr(config, "DEAO_OPEN_DATASET_ID", OPEN)
    monkeypatch.setattr(config, "DEAO_PARQUET_RESOURCE_ID", PARQUET_A)
    monkeypatch.setattr(config, "DEAO_CSV_RESOURCE_ID", CSV_A)
    monkeypatch.setattr(config, "DEAO_SCHEMA_RESOURCE_ID", SCHEMA_A)
    monkeypatch.setattr(config, "DEAO_CLOSED_PARQUET_RESOURCE_ID", PARQUET_B)
    monkeypatch.setattr(config, "DEAO_MIN_ROWS_TO_PUBLISH", 1)
    for s in registre:
        fake_datagouv.fichiers[s.resource_id] = _source_parquet(s.code)
    return SimpleNamespace(dist=dist, registre=registre, monkeypatch=monkeypatch)


def _registre(env, **par_code):
    env.monkeypatch.setattr(
        config,
        "DEAO_REGISTRE",
        tuple(dataclasses.replace(s, **par_code.get(s.code, {})) for s in env.registre),
    )


def _rid(env, code):
    return next(s.resource_id for s in env.registre if s.code == code)


def test_agregats_et_publication(fake_datagouv, env):
    deao_processing()
    assert {(d, r) for d, r, _ in fake_datagouv.publies} == {
        (OPEN, PARQUET_A),
        (OPEN, CSV_A),
        (OPEN, SCHEMA_A),
        ("ds-b", PARQUET_B),
    }
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp", "joue"}
    assert dict(closed.schema) == published_schema()
    assert (env.dist / "deao.csv.gz").exists()
    assert (env.dist / "schema.json").exists()


def test_source_absente_rien_publie(fake_datagouv, env):
    del fake_datagouv.fichiers[_rid(env, "joue")]
    with pytest.raises(Exception, match="joue"):
        deao_processing()
    assert fake_datagouv.publies == []


def test_source_sous_minimum_rien_publie(fake_datagouv, env):
    _registre(env, boamp={"min_rows": 5})
    with pytest.raises(Exception, match="boamp"):
        deao_processing()
    assert fake_datagouv.publies == []


def test_uid_en_double_rien_publie(fake_datagouv, env):
    fake_datagouv.fichiers[_rid(env, "joue")] = _source_parquet(
        "joue", uids=["boamp-0"]
    )
    with pytest.raises(Exception, match="uid"):
        deao_processing()
    assert fake_datagouv.publies == []


def test_schema_non_conforme_rien_publie(fake_datagouv, env):
    df = pl.read_parquet(io.BytesIO(fake_datagouv.fichiers[_rid(env, "joue")])).drop(
        "objet"
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    fake_datagouv.fichiers[_rid(env, "joue")] = buf.getvalue()
    with pytest.raises(Exception, match="schéma"):
        deao_processing()
    assert fake_datagouv.publies == []


def test_source_dataset_etranger_rien_publie(fake_datagouv, env):
    """Une ligne d'une autre source dans un fichier (ex. Closed Data dans boamp) bloque tout."""
    df = pl.read_parquet(io.BytesIO(fake_datagouv.fichiers[_rid(env, "boamp")]))
    df = df.with_columns(
        pl.when(pl.col("uid") == "boamp-0")
        .then(pl.lit("plateforme_fermee"))
        .otherwise(pl.col("sourceDataset"))
        .alias("sourceDataset")
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    fake_datagouv.fichiers[_rid(env, "boamp")] = buf.getvalue()
    with pytest.raises(RuntimeError, match="sourceDataset"):
        deao_processing()
    assert fake_datagouv.publies == []


@pytest.mark.parametrize(
    "variable",
    [
        "DEAO_OPEN_DATASET_ID",
        "DEAO_CLOSED_DATASET_ID",
        "DEAO_CLOSED_PARQUET_RESOURCE_ID",
    ],
)
def test_identifiant_vide_rien_publie(fake_datagouv, env, variable):
    env.monkeypatch.setattr(config, variable, "")
    with pytest.raises(RuntimeError, match=variable):
        deao_processing()
    assert fake_datagouv.publies == []


def test_identifiant_vide_sans_publication_ok(fake_datagouv, env):
    env.monkeypatch.setattr(config, "DEAO_OPEN_DATASET_ID", "")
    env.monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", ["x"])
    deao_processing()
    assert fake_datagouv.publies == []


def test_lecture_independante_de_deao_sources(fake_datagouv, env):
    from src.flows import scrap_open

    env.monkeypatch.setattr(scrap_open, "DEAO_SOURCES", ("boamp",))
    env.monkeypatch.setattr(config, "DEAO_SOURCES", ("boamp",), raising=False)
    deao_processing()
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp", "joue"}


def test_source_skip_non_lue(fake_datagouv, env):
    _registre(env, joue={"skip": True})
    deao_processing()
    assert not any(_rid(env, "joue") in url for url in fake_datagouv.appels)
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp"}


def test_agregat_open_data_restreint(fake_datagouv, env):
    _registre(env, joue={"open_data": False})
    deao_processing()
    ouvert = pl.read_parquet(env.dist / "deao.parquet")
    complet = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(ouvert["sourceDataset"]) == {"boamp"}
    assert set(complet["sourceDataset"]) == {"boamp", "joue"}


def test_solo_aucune_publication(fake_datagouv, env):
    env.monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", ["x"])
    deao_processing()
    assert fake_datagouv.publies == []
    assert (env.dist / "deao.parquet").exists()
    assert (env.dist / "closed" / "deao.parquet").exists()


def test_rapport_par_source():
    df = pl.concat(
        [pl.read_parquet(io.BytesIO(_source_parquet(c))) for c in ("boamp", "joue")]
    )
    indicateurs = {r["indicateur"]: r["valeur"] for r in lignes_rapport_sources(df)}
    assert indicateurs["lignes:boamp"] == 3
    assert indicateurs["statut:joue:ouvert"] == 3
    assert indicateurs["acheteur_id_source:boamp:siret"] == 3
    assert indicateurs["remplissage:objet:boamp"] == 1.0


def test_ancien_fichier_accepte_et_renomme(fake_datagouv, env):
    # fichier joue publié avant le déploiement : idAvisInitial au lieu de id,
    # sans les 8 nouvelles colonnes
    rid = _rid(env, "joue")
    df = pl.read_parquet(io.BytesIO(fake_datagouv.fichiers[rid]))
    df = df.rename({"id": "idAvisInitial"}).drop(
        "acheteur_codePostal",
        "urlDce",
        "urlReglementConsultation",
        "considerationsSociales",
        "considerationsEnvironnementales",
        "marcheReserve",
        "variantes",
        "groupementCommandes",
    )
    buf = io.BytesIO()
    df.write_parquet(buf)
    fake_datagouv.fichiers[rid] = buf.getvalue()
    deao_processing()
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert dict(closed.schema) == published_schema()


def test_dataset_a_sans_champs_fermes(fake_datagouv, env):
    deao_processing()
    ouvert = pl.read_parquet(env.dist / "deao.parquet")
    assert "urlDce" not in ouvert.columns and "id" in ouvert.columns
    assert dict(ouvert.schema) == open_schema()


def test_source_fermee_absente_non_bloquante(fake_datagouv, env):
    _registre(env, joue={"open_data": False})
    del fake_datagouv.fichiers[_rid(env, "joue")]
    deao_processing()
    assert (OPEN, PARQUET_A) in {(d, r) for d, r, _ in fake_datagouv.publies}
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp"}


def test_source_fermee_sous_minimum_non_bloquante(fake_datagouv, env):
    _registre(env, joue={"open_data": False, "min_rows": 99})
    deao_processing()
    assert ("ds-b", PARQUET_B) in {(d, r) for d, r, _ in fake_datagouv.publies}
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp"}


def test_source_fermee_ecartee_signalee(fake_datagouv, env, monkeypatch):
    _registre(env, joue={"open_data": False})
    del fake_datagouv.fichiers[_rid(env, "joue")]
    from src.flows import deao_processing as module

    lignes = []
    monkeypatch.setattr(
        module, "publier_rapport", lambda rapport: lignes.extend(rapport)
    )
    deao_processing()
    assert {"indicateur": "source_ecartee:joue", "valeur": 1} in lignes


def test_compteurs_dedoublonnage_dans_rapport(fake_datagouv, env, monkeypatch):
    from src.flows import deao_processing as module

    lignes = []
    monkeypatch.setattr(
        module, "publier_rapport", lambda rapport: lignes.extend(rapport)
    )
    deao_processing()
    assert {"indicateur": "dedoublonnage_retire", "valeur": 0} in lignes


def test_rapport_dce_seulement_avec_plateformes(fake_datagouv, env, monkeypatch):
    from src.flows import deao_processing as module

    lignes, appels = [], []
    monkeypatch.setattr(module, "publier_rapport", lambda r: lignes.extend(r))
    monkeypatch.setattr(
        module,
        "lignes_rapport_dce",
        lambda: appels.append(1) or [{"indicateur": "dce:nombre", "valeur": 7}],
    )
    deao_processing()
    assert appels == [] and not any(r["indicateur"].startswith("dce:") for r in lignes)

    _registre(env, joue={"open_data": False, "plateforme": "atexo"})
    deao_processing()
    assert appels == [1]
    assert {"indicateur": "dce:nombre", "valeur": 7} in lignes

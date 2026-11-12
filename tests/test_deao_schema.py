import json

import polars as pl
import pytest

from src.tasks.deao.schema import (
    AVIS_LOT_SCHEMA,
    PUBLISHED_FIELDS,
    conform_avis,
    empty_avis_frame,
    published_schema,
    write_table_schema,
)


def test_ordre_des_colonnes_publiees():
    noms = [f.name for f in PUBLISHED_FIELDS]
    assert len(noms) == 39
    assert noms[:6] == [
        "uid",
        "referenceAcheteur",
        "acheteur_id",
        "acheteur_id_source",
        "acheteur_nom",
        "objet",
    ]
    assert noms[29:32] == ["statut", "rectifie", "infosRectificatifs"]
    assert noms[-2:] == ["sourceDataset", "sourceFile"]
    assert list(published_schema()) == noms


def test_conform_avis_complete_et_caste():
    df = conform_avis(pl.DataFrame({"idAvis": ["26-1"], "dureeMois": [3]}))
    assert list(df.schema) == list(AVIS_LOT_SCHEMA)
    assert df.schema["dureeMois"] == pl.Int64 and df["objet"][0] is None


def test_conform_avis_refuse_colonne_inconnue():
    with pytest.raises(ValueError):
        conform_avis(pl.DataFrame({"inconnue": [1]}))


def test_empty_avis_frame():
    df = empty_avis_frame()
    assert df.height == 0 and dict(df.schema) == AVIS_LOT_SCHEMA


def test_write_table_schema(tmp_path):
    data = json.loads(write_table_schema(tmp_path / "schema.json").read_text())
    assert len(data["fields"]) == 39
    statut = next(f for f in data["fields"] if f["name"] == "statut")
    assert statut["enum"] == ["ouvert", "clos", "annule", "pre-information"]
    assert (
        next(f for f in data["fields"] if f["name"] == "dateLimiteReponse")["type"]
        == "datetime"
    )
    assert (
        next(f for f in data["fields"] if f["name"] == "techniques")["type"] == "array"
    )


def test_descriptions_mentionnent_les_deux_sources():
    champs = {f.name: f for f in PUBLISHED_FIELDS}
    for nom in ("urlAvis", "idAvisInitial", "idDernierAvis", "avisLies"):
        texte = champs[nom].description
        assert "boamp" in texte.lower() and "joue" in texte.lower(), nom
    assert "ted.europa.eu" in champs["urlAvis"].description
    assert "boamp.fr" in champs["urlAvis"].description
    for nom in ("uid", "sourceFile", "sourceDataset"):
        assert "joue" in champs[nom].description.lower(), nom


def _champs(chemin):
    with open(chemin, encoding="utf-8") as f:
        return json.load(f)["fields"]


def test_reference_au_format_du_schema_decp():
    from src.config import REFERENCE_DIR

    cles_decp = {
        c for champ in _champs(REFERENCE_DIR / "schema_base.json") for c in champ
    }
    for champ in _champs(REFERENCE_DIR / "schema_deao.json"):
        assert set(champ) <= cles_decp, champ["name"]
        assert {"type", "name", "title", "description", "short_title"} <= set(champ)


def test_colonnes_homonymes_meme_titre_court_que_decp():
    from src.config import REFERENCE_DIR

    decp = {c["name"]: c for c in _champs(REFERENCE_DIR / "schema_base.json")}
    for champ in _champs(REFERENCE_DIR / "schema_deao.json"):
        if champ["name"] in decp:
            assert champ["short_title"] == decp[champ["name"]]["short_title"], champ[
                "name"
            ]


def test_types_polars_deduits_de_la_reference():
    types = published_schema()
    assert types["dateLimiteReponse"] == pl.Datetime("us", "Europe/Paris")
    assert types["techniques"] == pl.List(pl.String)
    assert types["dureeMois"] == pl.Int64
    assert types["montantEstime"] == pl.Float64
    assert types["datePublicationInitiale"] == pl.Date
    assert types["rectifie"] == pl.Boolean


def test_schema_publie_identique_a_la_reference(tmp_path):
    from src.config import REFERENCE_DIR

    publie = _champs(write_table_schema(tmp_path / "schema.json"))
    assert publie == _champs(REFERENCE_DIR / "schema_deao.json")

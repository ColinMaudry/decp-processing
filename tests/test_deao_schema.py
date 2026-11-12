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

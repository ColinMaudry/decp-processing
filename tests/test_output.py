import json

import polars as pl

from src.tasks.output import generate_final_schema, restore_legacy_uid


def test_restore_legacy_uid_reconstruit_acheteur_id_plus_id():
    """L'uid publié doit revenir au format historique acheteur_id + id, sans le
    suffixe _codeCPV (transition pour un consommateur, cf. #186). La reconstruction
    depuis les colonnes gère aussi le cas codeCPV null (pas de '_' résiduel)."""
    lf = pl.LazyFrame(
        {
            "acheteur_id": ["ach1", "ach2"],
            "id": ["id_1", "id_2"],
            "codeCPV": ["12345678", None],
            "uid": ["ach1id_1_12345678", "ach2id_2_"],
            "montant": [100.0, 200.0],
        }
    )

    result = restore_legacy_uid(lf).collect()

    assert result["uid"].to_list() == ["ach1id_1", "ach2id_2"]
    # Les autres colonnes ne sont pas touchées
    assert result["montant"].to_list() == [100.0, 200.0]


def test_generate_final_schema_ecrit_dans_output_dir(tmp_path):
    """Régression : le schéma doit être écrit dans le output_dir fourni (le
    répertoire de travail work_dir de la bascule atomique), pas en dur dans
    DIST_DIR — sinon le fichier est détruit lors du rename work_dir -> DIST_DIR
    et publish_to_datagouv() ne le trouve pas."""
    lf = pl.LazyFrame({"objet": ["a"], "montant": ["100"]})

    generate_final_schema(lf, output_dir=tmp_path)

    schema_path = tmp_path / "schema.json"
    assert schema_path.exists(), "schema.json doit être écrit dans output_dir"
    # le fichier produit est un JSON valide avec une clé 'fields'
    with open(schema_path, encoding="utf-8") as f:
        assert "fields" in json.load(f)

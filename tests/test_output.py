import polars as pl

from src.tasks.output import restore_legacy_uid


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

import gzip
import json

import polars as pl
import pytest

from src.tasks.output import (
    generate_final_schema,
    restore_legacy_uid,
    save_to_files,
    sink_to_files,
    swap_dist,
)
from src.tasks.utils import gzip_file_reproducible


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


def test_generate_final_schema_suit_l_ordre_du_schema_de_base(tmp_path):
    """L'ordre des champs publiés doit être celui de schema_base.json — celui
    du Parquet et des colonnes affichées côté colibre.

    Il était alphabétique, effet de bord du `sorted()` qu'exige
    itertools.groupby pour fusionner les deux listes de champs.
    """
    from src.config import REFERENCE_DIR

    lf = pl.LazyFrame({"objet": ["a"], "montant": [100.0], "uid": ["x"]})

    generate_final_schema(lf, output_dir=tmp_path)

    with open(tmp_path / "schema.json", encoding="utf-8") as f:
        publies = [c["name"] for c in json.load(f)["fields"]]
    with open(REFERENCE_DIR / "schema_base.json", encoding="utf-8") as f:
        attendus = [c["name"] for c in json.load(f)["fields"]]

    assert publies == attendus


def test_swap_dist_remplace_l_existant(tmp_path):
    dist, wip = tmp_path / "dist", tmp_path / "dist_wip"
    dist.mkdir()
    (dist / "ancien.txt").write_text("a")
    wip.mkdir()
    (wip / "nouveau.txt").write_text("n")
    swap_dist(wip, dist)
    assert sorted(p.name for p in dist.iterdir()) == ["nouveau.txt"]
    assert not wip.exists() and not (tmp_path / "dist_old").exists()


def test_swap_dist_premier_run(tmp_path):
    wip = tmp_path / "dist_wip"
    wip.mkdir()
    (wip / "f").write_text("x")
    swap_dist(wip, tmp_path / "dist")
    assert (tmp_path / "dist" / "f").read_text() == "x"


def test_swap_dist_sans_wip_ne_touche_rien(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "f").write_text("x")
    with pytest.raises(FileNotFoundError):
        swap_dist(tmp_path / "dist_wip", dist)
    assert (dist / "f").read_text() == "x"


def test_sink_to_files_produit_decp_csv_gz_reproductible(tmp_path):
    """decp.csv (plusieurs Go en prod) est aussi produit en .csv.gz, avec un
    gzip reproductible (mtime=0, pas de nom embarqué) : checksum stable."""
    lf = pl.LazyFrame({"uid": ["a", "b"], "montant": [1.0, 2.0]})
    sink_to_files(lf, tmp_path / "decp")

    csv_path = tmp_path / "decp.csv"
    gz_path = tmp_path / "decp.csv.gz"
    assert csv_path.exists()
    assert (tmp_path / "decp.parquet").exists()
    assert gz_path.exists()
    gz_bytes = gz_path.read_bytes()
    assert gz_bytes[4:8] == b"\x00\x00\x00\x00"  # mtime=0 dans l'en-tête gzip
    assert not gz_bytes[3] & 0x08  # flag FNAME à 0 : pas de nom de fichier embarqué
    assert gzip.decompress(gz_bytes) == csv_path.read_bytes()
    # reproductible : même contenu -> mêmes octets
    autre = gzip_file_reproducible(csv_path, tmp_path / "autre.csv.gz")
    assert autre.read_bytes() == gz_bytes


def test_sink_to_files_autre_nom_ne_produit_pas_de_gz(tmp_path):
    lf = pl.LazyFrame({"a": [1]})
    sink_to_files(lf, tmp_path / "probabilites_naf_cpv")
    assert (tmp_path / "probabilites_naf_cpv.csv").exists()
    assert not (tmp_path / "probabilites_naf_cpv.csv.gz").exists()


def test_save_to_files_produit_decp_csv_gz(tmp_path):
    df = pl.DataFrame({"uid": ["a"], "montant": [1.0]})
    save_to_files(df, tmp_path / "decp", file_format=["csv"])
    assert (tmp_path / "decp.csv").exists()
    assert (tmp_path / "decp.csv.gz").exists()

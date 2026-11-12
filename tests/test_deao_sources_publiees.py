import polars as pl
import pytest

from src import config
from src.config import SourceDeao
from src.tasks.deao import sources_publiees as sp
from tests.deao_fakes import fake_datagouv  # noqa: F401

SOURCE = SourceDeao(
    code="joue", name="JOUE", open_data=True, resource_id="res-joue", min_rows=1
)


def _parquet(path):
    pl.DataFrame({"a": [1, 2]}).write_parquet(path)
    return path.read_bytes()


def test_telechargement_avec_cle(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = _parquet(tmp_path / "src.parquet")
    dest = tmp_path / "dest"
    dest.mkdir()
    chemin = sp.telecharger_fichier_source(SOURCE, dest)
    assert chemin == dest / "joue.parquet"
    assert pl.read_parquet(chemin)["a"].to_list() == [1, 2]
    assert len(fake_datagouv.appels) == 2
    assert fake_datagouv.api_key_vue == ["cle-test", "cle-test"]
    assert "/datasets/ds-b/resources/res-joue/" in fake_datagouv.appels[0]


def test_ressource_absente(fake_datagouv, tmp_path):  # noqa: F811
    with pytest.raises(sp.FichierSourceIndisponible, match="joue"):
        sp.telecharger_fichier_source(SOURCE, tmp_path)


def test_erreur_serveur_sur_fichier(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = b"x"
    fake_datagouv.en_erreur.add("res-joue")
    with pytest.raises(sp.FichierSourceIndisponible, match="joue"):
        sp.telecharger_fichier_source(SOURCE, tmp_path)


def test_fichier_illisible(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = b"pas un parquet"
    with pytest.raises(sp.FichierSourceIndisponible, match="joue"):
        sp.telecharger_fichier_source(SOURCE, tmp_path)


def test_aller_retour(fake_datagouv, tmp_path):  # noqa: F811
    p = tmp_path / "joue.parquet"
    _parquet(p)
    sp.publier_fichier("ds-b", "res-joue", p)
    assert fake_datagouv.publies == [("ds-b", "res-joue", "joue.parquet")]
    dest = tmp_path / "dest"
    dest.mkdir()
    lu = sp.telecharger_fichier_source(SOURCE, dest)
    assert pl.read_parquet(lu)["a"].to_list() == [1, 2]


@pytest.mark.parametrize(
    "publish, solo, attendu",
    [
        (True, [], True),
        (True, [""], True),
        (True, ["", "  "], True),
        (True, ["x"], False),
        (False, [], False),
    ],
)
def test_publication_autorisee(monkeypatch, publish, solo, attendu):
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", publish)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", solo)
    assert sp.publication_autorisee() is attendu


def test_publier_fichier_echec(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.echecs_publication.add("res-joue")
    p = tmp_path / "joue.parquet"
    _parquet(p)
    with pytest.raises(RuntimeError):
        sp.publier_fichier("ds-b", "res-joue", p)

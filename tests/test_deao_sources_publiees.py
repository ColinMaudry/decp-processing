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


def test_echec_telechargement_ne_laisse_aucun_fichier(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = b"pas un parquet"
    with pytest.raises(sp.FichierSourceIndisponible):
        sp.telecharger_fichier_source(SOURCE, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_echec_http_ne_laisse_aucun_fichier(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = b"x"
    fake_datagouv.en_erreur.add("res-joue")
    with pytest.raises(sp.FichierSourceIndisponible):
        sp.telecharger_fichier_source(SOURCE, tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_ancien_fichier_conserve_si_echec(fake_datagouv, tmp_path):  # noqa: F811
    ancien = tmp_path / "joue.parquet"
    _parquet(ancien)
    fake_datagouv.fichiers["res-joue"] = b"pas un parquet"
    with pytest.raises(sp.FichierSourceIndisponible):
        sp.telecharger_fichier_source(SOURCE, tmp_path)
    assert pl.read_parquet(ancien)["a"].to_list() == [1, 2]
    assert [f.name for f in tmp_path.iterdir()] == ["joue.parquet"]


def test_cle_api_pas_envoyee_a_un_hote_tiers(fake_datagouv, tmp_path):  # noqa: F811
    fake_datagouv.fichiers["res-joue"] = _parquet(tmp_path / "src.parquet")
    fake_datagouv.hote_fichiers = "cdn.exemple.org"
    dest = tmp_path / "dest"
    dest.mkdir()
    sp.telecharger_fichier_source(SOURCE, dest)
    assert fake_datagouv.api_key_vue == ["cle-test", None]


@pytest.mark.parametrize(
    "url, attendu",
    [
        ("https://www.data.gouv.fr/api/1/x", True),
        ("https://data.gouv.fr/x", True),
        ("https://static.data.gouv.fr/x", True),
        ("https://evildata.gouv.fr/x", False),
        ("https://data.gouv.fr.evil.org/x", False),
        ("https://exemple.org/data.gouv.fr", False),
    ],
)
def test_hote_datagouv(url, attendu):
    assert sp._est_hote_datagouv(url) is attendu


def test_ecrire_et_publier_ok(fake_datagouv, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    chemin = tmp_path / "joue.parquet"
    lignes = pl.DataFrame({"a": [1, 2]})
    assert sp.ecrire_et_publier("joue", lignes, SOURCE, chemin) is None
    assert chemin.exists()
    assert fake_datagouv.publies == [("ds-b", "res-joue", "joue.parquet")]


def test_ecrire_et_publier_sans_autorisation(fake_datagouv, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", False)
    chemin = tmp_path / "joue.parquet"
    assert (
        sp.ecrire_et_publier("joue", pl.DataFrame({"a": [1]}), SOURCE, chemin) is None
    )
    assert chemin.exists() and fake_datagouv.publies == []


def test_ecrire_et_publier_sous_minimum(fake_datagouv, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    source = SourceDeao("joue", "JOUE", True, "res-joue", 5)
    cause = sp.ecrire_et_publier(
        "joue", pl.DataFrame({"a": [1]}), source, tmp_path / "joue.parquet"
    )
    assert "sous le minimum (1 < 5)" in cause
    assert fake_datagouv.publies == []


def test_ecrire_et_publier_echec_publication(fake_datagouv, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    fake_datagouv.echecs_publication.add("res-joue")
    cause = sp.ecrire_et_publier(
        "joue", pl.DataFrame({"a": [1]}), SOURCE, tmp_path / "joue.parquet"
    )
    assert cause.startswith("publication en erreur")

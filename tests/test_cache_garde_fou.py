"""Tests du seuil de filesize par format et du garde-fou parquet vide en cache.

Contexte issue #199 : les dumps XML AIFE 2024-2025 ont produit des parquets de
0 ligne (bug de buffer corrigé en dee1df9), restés en cache indéfiniment.
"""

import polars as pl
import pytest

from src.config import MIN_FILESIZE
from src.tasks.get import get_clean


class TestMinFilesize:
    """Le seuil différencié bloque les fichiers trop petits pour contenir un marché."""

    def test_seuils_etablis(self):
        assert MIN_FILESIZE == {"json": 180, "xml": 1000}

    def test_fichier_json_petit_bloque_xml_autorise(self):
        """Un JSON de 179 o est filtré ; un XML de 1001 o passe (le plus petit
        XML avec un marché observé fait 1077 o)."""
        resources = [
            {"format": "json", "filesize": 179},
            {"format": "xml", "filesize": 1001},
        ]
        keep = [
            r for r in resources if r["filesize"] > MIN_FILESIZE.get(r["format"], 180)
        ]
        assert keep == [{"format": "xml", "filesize": 1001}]

    def test_xml_sous_le_seuil_bloque(self):
        """Un XML de 999 o (squelette sans marché possible) est filtré."""
        resources = [
            {"format": "xml", "filesize": 999},
            {"format": "xml", "filesize": 1077},  # plus petit XML avec 1 marché observé
        ]
        keep = [
            r for r in resources if r["filesize"] > MIN_FILESIZE.get(r["format"], 180)
        ]
        assert keep == [{"format": "xml", "filesize": 1077}]


class TestGetCleanCacheVide:
    """Un parquet en cache de hauteur 0 doit être re-traité, pas retourné tel quel."""

    RESOURCE = {
        "dataset_id": "test",
        "dataset_code": "test",
        "id": "r1",
        "ori_filename": "test.xml",
        "filename": "test_file",
        "url": "http://example.com/test.xml",
        "format": "xml",
        "checksum": "testchecksum",
        "filesize": 2000,
    }

    @pytest.fixture(autouse=True)
    def _cache_isole(self, tmp_path, monkeypatch):
        """Cache dirigé vers un répertoire temporaire et activé (les tests
        forcent DECP_USE_CACHE=false via pyproject.toml, ce qui court-circuite
        le cache) : les tests ne touchent pas le vrai data/resource_cache."""
        import src.tasks.get as get_mod

        cache_dir = tmp_path / "resource_cache"
        cache_dir.mkdir()
        monkeypatch.setattr(get_mod, "RESOURCE_CACHE_DIR", cache_dir)
        monkeypatch.setattr(get_mod, "DECP_USE_CACHE", True)

    def _cache_parquet(self, rows):
        """Écrit le parquet en cache via la même référence module que get_clean
        (le monkeypatch ne s'applique pas à un `from ... import` fait à la volée)."""
        import src.tasks.get as get_mod

        path = get_mod.RESOURCE_CACHE_DIR / "testchecksum.parquet"
        pl.DataFrame({"uid": ["x"] * rows}).write_parquet(path)
        return path

    def test_parquet_vide_en_cache_est_retraite(self, tmp_path, monkeypatch):
        """Cache contenant un parquet 0 ligne → re-téléchargement et écrasement."""
        self._cache_parquet(rows=0)
        reprocessed = []
        monkeypatch.setattr(
            "src.tasks.get.get_resource",
            lambda r, a: reprocessed.append(r["id"]) or (None, None),
        )
        result = get_clean(self.RESOURCE, [], {f"{self.RESOURCE['checksum']}.parquet"})
        assert reprocessed == ["r1"]
        # get_resource a renvoyé lf None → résultat None, mais la ressource a été
        # re-téléchargée : le parquet vide n'a pas servi de cache
        assert result is None

    def test_parquet_valide_en_cache_est_reutilise(self, tmp_path, monkeypatch):
        """Cache avec parquet non vide → pas de re-téléchargement."""
        self._cache_parquet(rows=5)
        called = []
        monkeypatch.setattr(
            "src.tasks.get.get_resource",
            lambda r, a: called.append(r["id"]) or (None, None),
        )
        result = get_clean(self.RESOURCE, [], {f"{self.RESOURCE['checksum']}.parquet"})
        assert called == []  # cache-hit, pas de re-téléchargement
        assert result is not None
        assert result.name == "testchecksum.parquet"

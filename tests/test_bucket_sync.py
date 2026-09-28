from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.tasks import bucket_sync

OLD = datetime(2026, 9, 1, tzinfo=timezone.utc)


class FakeS3:
    def __init__(self):
        self.objects = {}  # key -> {"Body": bytes, "LastModified": datetime}
        self.uploaded = []
        self.deleted = []

    def list_objects_v2(self, Bucket, Prefix, **kwargs):
        keys = [k for k in self.objects if k.startswith(Prefix)]
        return {
            "Contents": [
                {
                    "Key": k,
                    "Size": len(self.objects[k]["Body"]),
                    "LastModified": self.objects[k].get("LastModified", OLD),
                }
                for k in keys
            ]
        }

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.objects[Key] = {"Body": Path(Filename).read_bytes()}
        self.uploaded.append(Key)

    def put_object(self, Bucket, Key, Body=b""):
        self.objects[Key] = {"Body": Body}
        self.uploaded.append(Key)

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).write_bytes(self.objects[Key]["Body"])

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.deleted.append(Key)


@pytest.fixture
def fake_s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(bucket_sync, "_s3_client", lambda: fake)
    return fake


@pytest.fixture
def cache_dir(monkeypatch, tmp_path):
    cache = tmp_path / "resource_cache"
    cache.mkdir()
    monkeypatch.setattr(bucket_sync, "RESOURCE_CACHE_DIR", cache)
    return cache


def test_upload_resource_cache_miroir(fake_s3, cache_dir):
    (cache_dir / "aaa.parquet").write_bytes(b"aaa")
    fake_s3.objects["resource_cache/vieux.parquet"] = {"Body": b"vieux"}

    bucket_sync.upload_resource_cache()

    assert fake_s3.objects["resource_cache/aaa.parquet"]["Body"] == b"aaa"
    assert (
        "resource_cache/vieux.parquet" not in fake_s3.objects
    )  # absent localement → supprimé


def test_download_resource_cache_sauf_existant(fake_s3, cache_dir, tmp_path):
    fake_s3.objects["resource_cache/aaa.parquet"] = {"Body": b"aaa"}
    (cache_dir / "bbb.parquet").write_bytes(b"bbb")

    bucket_sync.download_resource_cache()

    assert (cache_dir / "aaa.parquet").read_bytes() == b"aaa"
    assert (cache_dir / "bbb.parquet").read_bytes() == b"bbb"  # pas écrasé


def test_download_resource_cache_taille_differente(fake_s3, cache_dir):
    fake_s3.objects["resource_cache/aaa.parquet"] = {"Body": b"longer content"}
    (cache_dir / "aaa.parquet").write_bytes(b"short")

    bucket_sync.download_resource_cache()

    assert (cache_dir / "aaa.parquet").read_bytes() == b"longer content"


def test_download_resource_cache_rien_si_vide(fake_s3, cache_dir):
    bucket_sync.download_resource_cache()  # bucket vide, ne doit pas lever
    assert list(cache_dir.iterdir()) == []


def test_download_resource_cache_pose_atime_last_modified(fake_s3, cache_dir):
    fake_s3.objects["resource_cache/aaa.parquet"] = {
        "Body": b"aaa",
        "LastModified": OLD,
    }

    bucket_sync.download_resource_cache()

    stat = (cache_dir / "aaa.parquet").stat()
    assert stat.st_atime == OLD.timestamp()
    assert stat.st_mtime == OLD.timestamp()


def test_upload_sirene_data(fake_s3, monkeypatch, tmp_path):
    sirene_dir = tmp_path / "sirene_2026-09"
    sirene_dir.mkdir()
    (sirene_dir / "unites_legales.parquet").write_bytes(b"ul")
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)

    bucket_sync.upload_sirene_data()

    assert (
        fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"]["Body"] == b"ul"
    )


def test_upload_sirene_data_ignore_temp_et_ecrit_marqueur(
    fake_s3, monkeypatch, tmp_path
):
    sirene_dir = tmp_path / "sirene_2026-09"
    sirene_dir.mkdir()
    (sirene_dir / "unites_legales.parquet").write_bytes(b"ul")
    (sirene_dir / "temp_enrich_acheteurs.parquet").write_bytes(b"tmp")
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)

    bucket_sync.upload_sirene_data()

    assert "sirene/sirene_2026-09/temp_enrich_acheteurs.parquet" not in fake_s3.objects
    assert fake_s3.uploaded[-1] == "sirene/sirene_2026-09/_COMPLETE"  # écrit en dernier


def test_upload_sirene_data_retire_marqueur_avant_modification(
    fake_s3, monkeypatch, tmp_path
):
    sirene_dir = tmp_path / "sirene_2026-09"
    sirene_dir.mkdir()
    (sirene_dir / "unites_legales.parquet").write_bytes(b"ul-nouveau")
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}
    fake_s3.objects["sirene/sirene_2026-09/_COMPLETE"] = {"Body": b""}

    bucket_sync.upload_sirene_data()

    assert fake_s3.deleted[0] == "sirene/sirene_2026-09/_COMPLETE"
    assert "sirene/sirene_2026-09/_COMPLETE" in fake_s3.objects  # réécrit à la fin


def test_upload_sirene_data_ne_supprime_pas_le_marqueur_comme_orphelin(
    fake_s3, monkeypatch, tmp_path
):
    sirene_dir = tmp_path / "sirene_2026-09"
    sirene_dir.mkdir()
    (sirene_dir / "unites_legales.parquet").write_bytes(b"ul")
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}
    fake_s3.objects["sirene/sirene_2026-09/_COMPLETE"] = {"Body": b""}

    bucket_sync.upload_sirene_data()  # rien à changer

    assert "sirene/sirene_2026-09/_COMPLETE" in fake_s3.objects
    assert fake_s3.deleted == []


def test_download_sirene_data(fake_s3, monkeypatch, tmp_path):
    sirene_dir = tmp_path / "sirene_2026-09"  # n'existe pas encore
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}
    fake_s3.objects["sirene/sirene_2026-09/_COMPLETE"] = {"Body": b""}

    bucket_sync.download_sirene_data()

    assert (sirene_dir / "unites_legales.parquet").read_bytes() == b"ul"


def test_download_sirene_data_noop_sans_marqueur(fake_s3, monkeypatch, tmp_path):
    sirene_dir = tmp_path / "sirene_2026-09"
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}

    bucket_sync.download_sirene_data()  # upload interrompu : pas de _COMPLETE

    assert not sirene_dir.exists()


def test_download_sirene_data_complet_sans_dossier_partiel(
    fake_s3, monkeypatch, tmp_path
):
    sirene_dir = tmp_path / "sirene_2026-09"
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}
    fake_s3.objects["sirene/sirene_2026-09/_COMPLETE"] = {"Body": b""}

    bucket_sync.download_sirene_data()

    assert (sirene_dir / "unites_legales.parquet").read_bytes() == b"ul"
    assert not (sirene_dir / "_COMPLETE").exists()
    assert not (tmp_path / "sirene_2026-09_partial").exists()


def test_download_sirene_data_noop_si_bucket_vide(fake_s3, monkeypatch, tmp_path):
    sirene_dir = tmp_path / "sirene_2026-09"
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)

    bucket_sync.download_sirene_data()  # ne doit pas lever

    assert not sirene_dir.exists()


def test_download_sirene_data_noop_si_local_existe(fake_s3, monkeypatch, tmp_path):
    sirene_dir = tmp_path / "sirene_2026-09"
    sirene_dir.mkdir()
    (sirene_dir / "unites_legales.parquet").write_bytes(b"deja-la")
    monkeypatch.setattr(bucket_sync, "SIRENE_DATA_DIR", sirene_dir)
    fake_s3.objects["sirene/sirene_2026-09/unites_legales.parquet"] = {"Body": b"ul"}
    fake_s3.objects["sirene/sirene_2026-09/_COMPLETE"] = {"Body": b""}

    bucket_sync.download_sirene_data()

    # rien de téléchargé
    assert (sirene_dir / "unites_legales.parquet").read_bytes() == b"deja-la"

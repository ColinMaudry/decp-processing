from pathlib import Path

import pytest

from src.tasks import bucket_sync


class FakeS3:
    """Double du client boto3 : dict (bucket, key) -> body."""

    def __init__(self):
        self.objects = {}  # key -> {"Body": bytes}
        self.uploaded = []

    def list_objects_v2(self, Bucket, Prefix):
        keys = [k for k in self.objects if k.startswith(Prefix)]
        return {
            "Contents": [{"Key": k, "Size": len(self.objects[k]["Body"])} for k in keys]
        }

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.objects[Key] = {"Body": Path(Filename).read_bytes()}
        self.uploaded.append(Key)

    def download_file(self, Bucket, Key, Filename):
        Path(Filename).write_bytes(self.objects[Key]["Body"])

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


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

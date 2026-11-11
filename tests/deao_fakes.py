"""Faux client S3 en mémoire, partagé par les tests DEAO."""

import io

import pytest
from botocore.exceptions import ClientError


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()

    def get_object(self, Bucket, Key, **kwargs):
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": Key}}, "GetObject"
            )
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return _Paginator(self)


class _Paginator:
    def __init__(self, fake):
        self.fake = fake

    def paginate(self, Bucket, Prefix=""):
        keys = sorted(k for k in self.fake.objects if k.startswith(Prefix))
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}


@pytest.fixture
def fake_s3(monkeypatch):
    from src.tasks.deao.sources.boamp import archive

    fake = FakeS3()
    monkeypatch.setattr(archive, "s3_client", lambda: fake)
    return fake

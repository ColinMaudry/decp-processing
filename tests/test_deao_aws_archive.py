from datetime import datetime

import pytest
from botocore.exceptions import ClientError

from src import config
from src.tasks.scrap.deao.atexo.dce import ErreurDepotS3
from src.tasks.scrap.deao.aws import archive


class FakeS3:
    def __init__(self, panne=None):
        self.objets = {}
        self.panne = panne

    def _verifier(self):
        if self.panne:
            raise self.panne

    def put_object(self, Bucket, Key, Body, **kw):
        self._verifier()
        self.objets[Key] = {"Body": Body, **kw}

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self._verifier()
        self.objets[Key] = {"Filename": Filename, **(ExtraArgs or {})}

    def head_object(self, Bucket, Key):
        self._verifier()
        if Key not in self.objets:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"Metadata": self.objets[Key].get("Metadata", {})}

    def get_object(self, Bucket, Key):
        self._verifier()
        if Key not in self.objets:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        o = self.objets[Key]

        class Corps:
            def read(self_):
                return o["Body"]

        return {"Body": Corps(), "Metadata": o.get("Metadata", {})}


@pytest.fixture
def s3(monkeypatch):
    faux = FakeS3()
    monkeypatch.setattr(archive, "s3_client", lambda: faux)
    return faux


def test_cles():
    assert archive.cle_avis("1902107") == "www.marches-publics.info/raw/1902107.html"
    assert (
        archive.cle_rc("www.marches-publics.info", "1902107")
        == "rc/www.marches-publics.info/1902107.pdf"
    )


def test_marque_date_limite():
    assert archive.marque_date_limite(datetime(2026, 3, 4, 12, 30, 59)) == (
        "2026-03-04T12:30"
    )
    assert archive.marque_date_limite(None) == ""


def test_deposer_avis_prive_avec_metadonnee(s3):
    archive.deposer_avis("1", "<p>é</p>", "2026-03-04T12:30")
    o = s3.objets[archive.cle_avis("1")]
    assert o["Body"] == "<p>é</p>".encode("utf-8")
    assert o["ACL"] == "private"
    assert o["ContentType"] == "text/html; charset=utf-8"
    assert o["Metadata"] == {"date-limite": "2026-03-04T12:30"}


def test_lire_avis_archive(s3):
    archive.deposer_avis("1", "<p>é</p>", "2026-03-04T12:30")
    assert archive.lire_avis_archive("1", "2026-03-04T12:30") == "<p>é</p>"
    assert archive.lire_avis_archive("1", "2026-03-05T12:30") is None
    assert archive.lire_avis_archive("1", None) == "<p>é</p>"
    assert archive.lire_avis_archive("2", None) is None
    assert archive.lire_avis_archive("2", "x") is None


def test_deposer_rc_public_et_supprime(s3, tmp_path):
    f = tmp_path / "rc.pdf"
    f.write_bytes(b"%PDF")
    cle = archive.cle_rc("www.marches-publics.info", "1")
    url = archive.deposer_rc(f, cle)
    assert url.endswith(cle)
    o = s3.objets[cle]
    assert o["ACL"] == "public-read" and o["ContentType"] == "application/pdf"
    assert not f.exists()


def test_deposer_rc_echec_supprime_quand_meme(monkeypatch, tmp_path):
    monkeypatch.setattr(
        archive, "s3_client", lambda: FakeS3(panne=RuntimeError("boom"))
    )
    f = tmp_path / "rc.pdf"
    f.write_bytes(b"%PDF")
    with pytest.raises(ErreurDepotS3):
        archive.deposer_rc(f, "rc/x/1.pdf")
    assert not f.exists()


def test_panne_s3(monkeypatch):
    err = ClientError({"Error": {"Code": "500"}}, "GetObject")
    monkeypatch.setattr(archive, "s3_client", lambda: FakeS3(panne=err))
    with pytest.raises(ErreurDepotS3):
        archive.lire_avis_archive("1", None)
    with pytest.raises(ErreurDepotS3):
        archive.deposer_avis("1", "x", "")


def test_config_relire(monkeypatch):
    assert config.PLATEFORMES_DEAO == {"atexo", "aws"}
    assert isinstance(config.DEAO_AWS_RELIRE_ARCHIVE, bool)

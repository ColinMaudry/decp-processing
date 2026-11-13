import pytest

from src.tasks.scrap.deao.atexo import dce
from src.tasks.scrap.deao.atexo.client import DceTelecharge
from tests.deao_fakes import FakeS3


@pytest.fixture
def fake_s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(dce, "s3_client", lambda: fake)
    monkeypatch.setattr(dce.config, "S3_BUCKET", "b")
    monkeypatch.setattr(dce.config, "S3_ENDPOINT_URL", "https://s3.example.com/")
    monkeypatch.setattr(dce.config, "DEAO_DCE_S3_PREFIX", "dce")
    return fake


def _dce(tmp_path):
    chemin = tmp_path / "1-DCE.zip"
    chemin.write_bytes(b"zipdata")
    return DceTelecharge(chemin, "1-DCE.zip", "application/zip", None)


def test_cle_dce(fake_s3):
    assert (
        dce.cle_dce("megalis_bretagne", "242752", "DCE_26190042.ZIP")
        == "dce/megalis_bretagne/242752.zip"
    )


def test_cle_dce_sans_extension(fake_s3):
    assert dce.cle_dce("maximilien", "9", "dce").endswith("/9.zip")


def test_url_publique(fake_s3):
    assert dce.url_publique("dce/x/1.zip") == "https://s3.example.com/b/dce/x/1.zip"


def test_cle_depuis_url(fake_s3):
    assert dce.cle_depuis_url(dce.url_publique("dce/x/1.zip")) == "dce/x/1.zip"
    assert dce.cle_depuis_url("https://autre.example.com/b/dce/x/1.zip") is None
    assert dce.cle_depuis_url(None) is None


def test_depot_public_et_metadonnees(fake_s3, tmp_path):
    d = _dce(tmp_path)
    url = dce.deposer_dce(d, "dce/x/1.zip", "72,09 Mo")
    assert url == "https://s3.example.com/b/dce/x/1.zip"
    assert fake_s3.objects["dce/x/1.zip"] == b"zipdata"
    assert fake_s3.extra_args["dce/x/1.zip"] == {
        "ACL": "public-read",
        "ContentType": "application/zip",
        "ContentDisposition": (
            "attachment; filename=\"1-DCE.zip\"; filename*=UTF-8''1-DCE.zip"
        ),
        "Metadata": {"taille-affichee": "72,09 Mo"},
    }
    assert not d.chemin.exists()
    assert dce.taille_archivee("dce/x/1.zip") == "72,09 Mo"


def test_taille_archivee_absente(fake_s3):
    assert dce.taille_archivee("dce/x/404.zip") is None


def test_depot_supprime_fichier_si_echec(fake_s3, tmp_path, monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("panne S3")

    monkeypatch.setattr(fake_s3, "upload_file", boom)
    d = _dce(tmp_path)
    with pytest.raises(RuntimeError):
        dce.deposer_dce(d, "dce/x/1.zip", None)
    assert not d.chemin.exists()


def test_content_disposition_nom_hostile():
    entete = dce._content_disposition('a"b\\\r\né.zip')
    assert entete == (
        "attachment; filename=\"ab_.zip\"; filename*=UTF-8''a%22b%5C%0D%0A%C3%A9.zip"
    )
    assert entete.isascii() and "\r" not in entete and "\n" not in entete


def test_normaliser_taille():
    assert dce.normaliser_taille("72,09 Mo") == "72,09 Mo"
    assert dce.normaliser_taille(" 72,09  Mo ") == "72,09 Mo"
    assert dce.normaliser_taille("é").isascii()
    assert dce.normaliser_taille(None) is None


def test_depot_taille_nbsp_coherente(fake_s3, tmp_path):
    dce.deposer_dce(_dce(tmp_path), "dce/x/1.zip", "72,09 Mo")
    assert dce.taille_archivee("dce/x/1.zip") == dce.normaliser_taille("72,09 Mo")


def test_cle_dce_suffixe_non_sur(fake_s3):
    assert dce.cle_dce("x", "1", "a.z ip/../é").endswith("/1.zip")

import pytest
from botocore.exceptions import ClientError

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
    monkeypatch.setattr(dce.config, "DEAO_DCE_URL_PUBLIQUE", "")
    return fake


def _dce(tmp_path):
    chemin = tmp_path / "1-DCE.zip"
    chemin.write_bytes(b"zipdata")
    return DceTelecharge(chemin, "1-DCE.zip", "application/zip", None)


def test_cle_dce(fake_s3):
    assert (
        dce.cle_dce("marches.megalis.bretagne.bzh", "242752", "DCE_26190042.ZIP")
        == "dce/marches.megalis.bretagne.bzh/242752.zip"
    )


def test_cle_dce_sans_extension(fake_s3):
    assert dce.cle_dce("marches.maximilien.fr", "9", "dce").endswith("/9.zip")


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
    with pytest.raises(dce.ErreurDepotS3, match="panne S3"):
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


def _refus(operation):
    def refus(**kwargs):
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "refusé"}}, operation
        )

    return refus


def test_taille_archivee_erreur_s3_leve_erreur_depot(fake_s3, monkeypatch):
    monkeypatch.setattr(fake_s3, "head_object", _refus("HeadObject"))
    with pytest.raises(dce.ErreurDepotS3):
        dce.taille_archivee("dce/x/1.zip")


def test_verifier_depot_s3(fake_s3, monkeypatch):
    deposes = []
    reel = fake_s3.put_object

    def put_object(**kwargs):
        deposes.append(kwargs)
        reel(**kwargs)

    monkeypatch.setattr(fake_s3, "put_object", put_object)
    dce.verifier_depot_s3("marches.maximilien.fr")
    # Objet de contrôle sous le préfixe des DCE, en lecture publique, puis supprimé.
    assert deposes[0]["Key"].startswith("dce/")
    assert deposes[0]["ACL"] == "public-read"
    assert fake_s3.objects == {}


def test_verifier_depot_s3_acl_refusee(fake_s3, monkeypatch):
    monkeypatch.setattr(fake_s3, "put_object", _refus("PutObject"))
    with pytest.raises(dce.ErreurDepotS3, match="AccessDenied"):
        dce.verifier_depot_s3("marches.maximilien.fr")


def test_url_publique_base_personnalisee(fake_s3, monkeypatch):
    monkeypatch.setattr(
        dce.config, "DEAO_DCE_URL_PUBLIQUE", "https://s3.example.com/compte:b/"
    )
    url = dce.url_publique("dce/x/1.zip")
    assert url == "https://s3.example.com/compte:b/dce/x/1.zip"
    assert dce.cle_depuis_url(url) == "dce/x/1.zip"
    # Une URL au format par défaut n'est plus reconnue.
    assert dce.cle_depuis_url("https://s3.example.com/b/dce/x/1.zip") is None


def test_verifier_depot_s3_cle_propre_a_la_plateforme(fake_s3, monkeypatch):
    cles = []
    reel = fake_s3.put_object

    def put_object(**kwargs):
        cles.append(kwargs["Key"])
        reel(**kwargs)

    monkeypatch.setattr(fake_s3, "put_object", put_object)
    dce.verifier_depot_s3("marches.maximilien.fr")
    dce.verifier_depot_s3("marches.megalis.bretagne.bzh")
    assert cles == [
        "dce/.controle-depot-marches.maximilien.fr",
        "dce/.controle-depot-marches.megalis.bretagne.bzh",
    ]

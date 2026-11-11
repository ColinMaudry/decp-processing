import boto3
import pytest

from src.tasks import utils
from src.tasks.utils import s3_client, siret_valide


@pytest.mark.parametrize(
    "s, attendu",
    [
        ("83942912300020", True),  # Luhn OK
        ("32384135300911", True),
        ("35600000000010", True),  # La Poste : Luhn KO mais somme % 5 == 0
        ("35600000000011", False),  # La Poste, somme % 5 != 0
        ("12345678900001", False),  # Luhn KO
        ("8394291230002", False),  # 13 chiffres
        ("8394291230002A", False),
        ("", False),
        (None, False),
    ],
)
def test_siret_valide(s, attendu):
    assert siret_valide(s) is attendu


def test_s3_client_session_dediee(monkeypatch):
    """Une session boto3 par client : la session par défaut n'est pas thread-safe."""
    sessions = []

    class FakeSession:
        def __init__(self):
            sessions.append(self)
            self.appels = []

        def client(self, service, **kwargs):
            self.appels.append((service, kwargs))
            return f"client-{len(sessions)}"

    def interdit(*args, **kwargs):
        raise AssertionError("boto3.client (session par défaut) interdit")

    monkeypatch.setattr(utils, "check_s3_config", lambda: [])
    monkeypatch.setattr(boto3.session, "Session", FakeSession)
    monkeypatch.setattr(boto3, "client", interdit)

    assert s3_client() == "client-1"
    assert s3_client() == "client-2"
    assert len(sessions) == 2
    service, kwargs = sessions[0].appels[0]
    assert service == "s3"
    assert kwargs["config"].signature_version == "s3v4"
    assert kwargs["config"].s3 == {"addressing_style": "path"}
    assert set(kwargs) >= {
        "endpoint_url",
        "region_name",
        "aws_access_key_id",
        "aws_secret_access_key",
    }


def test_s3_client_config_manquante(monkeypatch):
    monkeypatch.setattr(utils, "check_s3_config", lambda: ["S3_BUCKET"])
    with pytest.raises(ValueError, match="S3_BUCKET"):
        s3_client()

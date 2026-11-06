"""Garde d'environnement du lanceur : fail-fast avant toute création d'instance."""

import pytest

from src.flows.launcher import _public_key, valider_env_instance
from src.infra.remote_run import RemoteRunError


def test_valider_env_instance_accepte_une_url_publique(monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "https://prefect.maudry.com/api")
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "secret")
    valider_env_instance()  # ne lève rien


@pytest.mark.parametrize(
    "url", ["", "prefect.maudry.com/api", "ftp://prefect.example.com"]
)
def test_valider_env_instance_rejette_une_url_absente_ou_malformee(monkeypatch, url):
    monkeypatch.setenv("PREFECT_API_URL", url)
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "secret")
    with pytest.raises(RemoteRunError, match="PREFECT_API_URL"):
        valider_env_instance()


@pytest.mark.parametrize(
    "url", ["http://localhost:4200/api", "http://127.0.0.1:4200/api"]
)
def test_valider_env_instance_rejette_une_url_locale(monkeypatch, url):
    monkeypatch.setenv("PREFECT_API_URL", url)
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "secret")
    with pytest.raises(RemoteRunError, match="localhost ou 127.0.0.1"):
        valider_env_instance()


def test_valider_env_instance_rejette_une_auth_absente(monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "https://prefect.maudry.com/api")
    monkeypatch.delenv("PREFECT_API_AUTH_STRING", raising=False)
    with pytest.raises(RemoteRunError, match="PREFECT_API_AUTH_STRING"):
        valider_env_instance()


def test_public_key_lit_le_fichier_pub(monkeypatch, tmp_path):
    pub = tmp_path / "id_ed25519.pub"
    pub.write_text(
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIABtest decp-launcher\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("src.flows.launcher.SCALEWAY_SSH_PUBLIC_KEY_PATH", pub)
    assert _public_key() == "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIABtest decp-launcher"


def test_public_key_absente_echoue_avec_un_message_clair(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "src.flows.launcher.SCALEWAY_SSH_PUBLIC_KEY_PATH",
        tmp_path / "inexistant.pub",
    )
    with pytest.raises(RuntimeError, match="SCALEWAY_SSH_PUBLIC_KEY_PATH"):
        _public_key()


def test_public_key_rejette_une_cle_privee(monkeypatch, tmp_path):
    faux = tmp_path / "id_ed25519"
    faux.write_text(
        "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAA\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("src.flows.launcher.SCALEWAY_SSH_PUBLIC_KEY_PATH", faux)
    with pytest.raises(RuntimeError, match="pas la clé privée"):
        _public_key()

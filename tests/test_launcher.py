"""Garde d'environnement du lanceur : fail-fast avant toute création d'instance."""

import pytest

from src.flows.launcher import valider_env_instance
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

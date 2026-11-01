import pytest

from src.infra.user_data import render_user_data

ARGS = dict(
    env="prod",
    branch="main",
    volume_id="11111111-2222-3333-4444-555555555555",
    ssh_public_key="ssh-ed25519 AAAAtest decp-launcher",
    env_vars={"DECP_DATA_DIR": "/data", "DATAGOUVFR_API_KEY": "secret-123"},
)


def test_user_data_contient_branche_pool_volume_et_cle():
    ud = render_user_data(**ARGS)
    assert ud.startswith("#!/bin/bash")
    assert "--branch main" in ud
    assert "--pool scaleway-prod" in ud
    assert "11111111-2222-3333-4444-555555555555" in ud
    assert "ssh-ed25519 AAAAtest decp-launcher" in ud
    assert "uv sync --frozen" in ud


def test_user_data_ecrit_les_variables_en_fichier_600():
    ud = render_user_data(**ARGS)
    assert "DATAGOUVFR_API_KEY='secret-123'" in ud
    assert "chmod 600 /etc/decp.env" in ud


def test_user_data_ne_formate_que_si_vierge():
    ud = render_user_data(**ARGS)
    assert "blkid" in ud
    assert ud.index("blkid") < ud.index("mkfs.ext4")


def test_user_data_refuse_une_valeur_avec_quote():
    with pytest.raises(ValueError):
        render_user_data(**{**ARGS, "env_vars": {"X": "a'b"}})


def test_user_data_refuse_une_cle_avec_quote():
    with pytest.raises(ValueError, match="Clé interdite"):
        render_user_data(**{**ARGS, "env_vars": {**ARGS["env_vars"], "MA'CLE": "v"}})


def test_user_data_refuse_une_cle_avec_saut_de_ligne():
    with pytest.raises(ValueError, match="Clé interdite"):
        render_user_data(**{**ARGS, "env_vars": {**ARGS["env_vars"], "MA\nCLE": "v"}})


@pytest.mark.parametrize(
    "cle_reservee", ["ENV", "BRANCH", "VOLUME_ID", "SSH_PUBLIC_KEY", "ENV_FILE"]
)
def test_user_data_refuse_une_cle_reservee(cle_reservee):
    with pytest.raises(ValueError, match="Clé réservée"):
        render_user_data(
            **{**ARGS, "env_vars": {**ARGS["env_vars"], cle_reservee: "v"}}
        )

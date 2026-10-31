from pathlib import Path
from string import Template

MODELE = Path(__file__).with_name("cloud-init.sh")


def render_user_data(
    *,
    env: str,
    branch: str,
    volume_id: str,
    ssh_public_key: str,
    env_vars: dict[str, str],
) -> str:
    """Rend le script cloud-init (user-data) de l'instance éphémère Scaleway."""
    valeurs = {
        "ENV": env,
        "BRANCH": branch,
        "VOLUME_ID": volume_id,
        "SSH_PUBLIC_KEY": ssh_public_key,
        **env_vars,
    }
    for cle, valeur in valeurs.items():
        if "'" in valeur or "\n" in valeur:
            raise ValueError(f"Valeur interdite (quote ou saut de ligne) pour {cle}")
    env_file = "\n".join(f"{cle}='{valeur}'" for cle, valeur in env_vars.items())
    return Template(MODELE.read_text()).substitute(
        ENV=env,
        BRANCH=branch,
        VOLUME_ID=volume_id,
        SSH_PUBLIC_KEY=ssh_public_key,
        ENV_FILE=env_file,
    )

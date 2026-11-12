from prefect import flow
from prefect.runner.storage import GitRepository

# Rappel : pour déployer, PREFECT_API_URL doit être non null, sinon on déploie
# sur une instance éphémère.
#
# Architecture des déploiements lourds DECP (#189) :
# - les déploiements lanceurs (pool `local`, serveur actuel) appellent
#   `run_on_scaleway`, qui crée une instance Scaleway éphémère et déclenche le
#   déploiement `*-exec-*` correspondant ;
# - les déploiements `*-exec-*` (pools `scaleway-prod`, `scaleway-dev`) sont
#   exécutés par le worker Prefect de l'instance éphémère ;
# - les variables d'environnement d'instance (DECP_DIST_DIR, DECP_DATA_DIR)
#   viennent du user-data de l'instance, pas des `job_variables`.
#
# Les déploiements DEAO, eux, tournent directement sur le pool `local`
# (serveur principal), sans instance Scaleway.
#
# Tags (voir la spec DEAO) : produit (`attributions`, `appels-offres`), étape
# (`scrap`, `traitement`, `referentiel`), source, environnement (`prod`, `dev`).

REPO_URL = "https://github.com/ColinMaudry/decp-processing.git"
LAUNCHER = "src/flows/launcher.py:run_on_scaleway"


def _env_tag(branch: str) -> str:
    return "dev" if branch == "dev" else "prod"


def deao_deployment_specs() -> list[dict]:
    """Déploiements DEAO : kwargs de `.deploy()` + `entrypoint` et `branch` (source Git).

    Pool `local`. DEAO a son propre `dist_dir` (/srv/shared/deao/...), distinct de
    celui des DECP.
    """
    flows = [
        # (nom, entrypoint, cron prod, concurrency_limit, tags, description)
        (
            "scrap-open",
            "src/flows/scrap_open.py:scrap_open",
            "0 8,12,16 * * *",
            1,
            ["appels-offres", "scrap", "boamp", "joue"],
            "DEAO : collecte BOAMP/JOUE (appels d'offres ouverts), puis traitement.",
        ),
        (
            "scrap-closed",
            "src/flows/scrap_closed.py:scrap_closed",
            "0 6 * * *",
            1,
            ["appels-offres", "scrap"],
            "DEAO : scrapers de plateformes (appels d'offres clos), puis traitement.",
        ),
        (
            "deao-processing",
            "src/flows/deao_processing.py:deao_processing",
            None,
            None,
            ["appels-offres", "traitement"],
            "DEAO : agrégation et publication des datasets (lancement manuel).",
        ),
    ]
    specs = []
    for nom, entrypoint, cron, limite, tags, description in flows:
        for env in ("prod", "dev"):
            spec = {
                "entrypoint": entrypoint,
                "branch": "main" if env == "prod" else "dev",
                "name": nom if env == "prod" else f"{nom}-dev",
                "description": description
                if env == "prod"
                else f"{description} Branche dev, sans publication.",
                "work_pool_name": "local",
                "tags": [*tags, env],
                "job_variables": {
                    "env": {
                        "DECP_PROCESSING_PUBLISH": str(env == "prod"),
                        "DEAO_DATA_DIR": f"/home/yunohost.app/prefect/deao/{env}",
                        "DEAO_DIST_DIR": f"/srv/shared/deao/{env}/dist",
                    }
                },
            }
            if env == "prod" and cron:
                spec["cron"] = cron
            if limite is not None:
                spec["concurrency_limit"] = limite
            specs.append(spec)
    return specs


def decp_deployment_specs() -> list[dict]:
    """Déploiements DECP : kwargs de `.deploy()` + `entrypoint` et `branch`."""
    decp = "src/flows/decp_processing.py:decp_processing"
    sirene = "src/flows/sirene_preprocess.py:sirene_preprocess"
    scrap = "src/flows/scrap.py:scrap"
    tags_decp = ["attributions", "traitement"]
    tags_sirene = ["referentiel"]
    tags_scrap = ["attributions", "scrap"]

    def scrap_env(dist_env, refresh, mode=None, cible=None):
        env = {
            "DECP_PROCESSING_PUBLISH": "True",
            "DECP_DIST_DIR": f"/srv/shared/decp/{dist_env}/dist",
            "PREFECT_TASKS_REFRESH_CACHE": refresh,
        }
        if cible:
            env["SCRAPING_MODE"] = mode
            env["SCRAPING_TARGET"] = cible
        return {"env": env}

    specs = [
        {
            "entrypoint": LAUNCHER,
            "branch": "main",
            "name": "decp-processing",
            "description": "Tous les jours à 6h00, sur une instance Scaleway éphémère.",
            "work_pool_name": "local",
            "cron": "0 5 * * *",
            "parameters": {
                "env": "prod",
                "deployment": "decp-processing/decp-processing-exec-prod",
                "branch": "main",
                "dist_dir": "/srv/shared/decp/prod/dist",
            },
            "tags": tags_decp,
        },
        {
            "entrypoint": decp,
            "branch": "main",
            "name": "decp-processing-exec-prod",
            "description": "Exécution de decp_processing sur l'instance Scaleway éphémère de prod.",
            "work_pool_name": "scaleway-prod",
            "job_variables": {
                "env": {
                    "DECP_PROCESSING_PUBLISH": "True",
                    "DECP_USE_CACHE": "True",
                }
            },
            "tags": tags_decp,
        },
        {
            "entrypoint": LAUNCHER,
            "branch": "main",
            "name": "decp-processing-dev",
            "description": "Déploiement de la branche dev, sur une instance Scaleway éphémère.",
            "work_pool_name": "local",
            "parameters": {
                "env": "dev",
                "deployment": "decp-processing/decp-processing-exec-dev",
                "branch": "dev",
                "dist_dir": "/srv/shared/decp/dev/dist",
            },
            "tags": tags_decp,
        },
        {
            "entrypoint": decp,
            "branch": "dev",
            "name": "decp-processing-exec-dev",
            "description": "Exécution de decp_processing sur l'instance Scaleway éphémère de dev.",
            "work_pool_name": "scaleway-dev",
            "job_variables": {
                "env": {
                    "DECP_PROCESSING_PUBLISH": "False",
                    "DECP_USE_CACHE": "False",
                }
            },
            "tags": tags_decp,
        },
        {
            "entrypoint": LAUNCHER,
            "branch": "main",
            "name": "sirene-preprocess",
            "description": "Préparation des données SIRENE. Tous les mois, le 3, sur une instance Scaleway éphémère.",
            "work_pool_name": "local",
            "cron": "0 1 3 * *",
            "parameters": {
                "env": "prod",
                "deployment": "sirene-preprocess/sirene-preprocess-exec-prod",
                "branch": "main",
                "dist_dir": None,
            },
            "tags": tags_sirene,
        },
        {
            "entrypoint": sirene,
            "branch": "main",
            "name": "sirene-preprocess-exec-prod",
            "description": "Prétraitement SIRENE sur l'instance Scaleway éphémère de prod.",
            "work_pool_name": "scaleway-prod",
            "job_variables": {"env": {"PREFECT_TASKS_REFRESH_CACHE": "False"}},
            "tags": tags_sirene,
        },
        {
            "entrypoint": LAUNCHER,
            "branch": "main",
            "name": "sirene-preprocess-dev",
            "description": "Préparation des données SIRENE, sur une instance Scaleway éphémère.",
            "work_pool_name": "local",
            "parameters": {
                "env": "dev",
                "deployment": "sirene-preprocess/sirene-preprocess-exec-dev",
                "branch": "dev",
                "dist_dir": None,
            },
            "tags": tags_sirene,
        },
        {
            "entrypoint": sirene,
            "branch": "dev",
            "name": "sirene-preprocess-exec-dev",
            "description": "Prétraitement SIRENE sur l'instance Scaleway éphémère de dev.",
            "work_pool_name": "scaleway-dev",
            "tags": tags_sirene,
        },
        {
            "entrypoint": scrap,
            "branch": "main",
            "name": "scrap-marches-securises",
            "description": "Scraping des données de marches-securises.fr.",
            "work_pool_name": "local",
            "cron": "0 0 * * 1-5",
            "job_variables": scrap_env(
                "prod", "False", "month", "marches-securises.fr"
            ),
            "tags": [*tags_scrap, "marches-securises"],
        },
        {
            "entrypoint": scrap,
            "branch": "main",
            "name": "scrap-aws",
            "description": "Scraping des données de marches-publics.info.",
            "work_pool_name": "local",
            "cron": "0 0 * * 1-5",
            "job_variables": scrap_env("prod", "False", "month", "aws"),
            "tags": [*tags_scrap, "aws"],
        },
        {
            "entrypoint": scrap,
            "branch": "main",
            "name": "scrap-dume",
            "description": "Scraping des données de l'API DUME.",
            "work_pool_name": "local",
            "cron": "0 0 * * 2",
            "job_variables": scrap_env("prod", "False", "month", "dume"),
            "tags": [*tags_scrap, "dume"],
        },
        {
            "entrypoint": scrap,
            "branch": "main",
            "name": "scrap",
            "description": "Scraping des données.",
            "work_pool_name": "local",
            "job_variables": scrap_env("prod", "True"),
            "tags": tags_scrap,
        },
        {
            "entrypoint": scrap,
            "branch": "dev",
            "name": "scrap-dev",
            "description": "Scraping des données.",
            "work_pool_name": "local",
            "job_variables": scrap_env("dev", "True"),
            "tags": tags_scrap,
        },
    ]
    for spec in specs:
        spec["tags"] = [*spec["tags"], _env_tag(_env_branch(spec))]
    return specs


def _env_branch(spec: dict) -> str:
    """Branche déterminant l'environnement : celle du déploiement, sauf pour les
    lanceurs `*-dev` (source `main`) dont le paramètre `branch` vaut `dev`."""
    return spec.get("parameters", {}).get("branch", spec["branch"])


if __name__ == "__main__":
    for spec in decp_deployment_specs() + deao_deployment_specs():
        flow.from_source(
            source=GitRepository(url=REPO_URL, branch=spec["branch"]),
            entrypoint=spec["entrypoint"],
        ).deploy(
            **{k: v for k, v in spec.items() if k not in ("branch", "entrypoint")},
            ignore_warnings=True,
        )

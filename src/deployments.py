from prefect import flow
from prefect.runner.storage import GitRepository

# Rappel : pour déployer, PREFECT_API_URL doit être non null, sinon on déploie
# sur une instance éphémère.
#
# Architecture des déploiements lourds (#189) :
# - les déploiements lanceurs (pool `local`, serveur actuel) appellent
#   `run_on_scaleway`, qui crée une instance Scaleway éphémère et déclenche le
#   déploiement `*-exec-*` correspondant ;
# - les déploiements `*-exec-*` (pools `scaleway-prod`, `scaleway-dev`) sont
#   exécutés par le worker Prefect de l'instance éphémère ;
# - les variables d'environnement d'instance (DECP_DIST_DIR, DECP_DATA_DIR)
#   viennent du user-data de l'instance, pas des `job_variables`.

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/launcher.py:run_on_scaleway",
    ).deploy(
        name="decp-processing",
        description="Tous les jours à 6h00, sur une instance Scaleway éphémère.",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 5 * * *",
        parameters={
            "env": "prod",
            "deployment": "decp-processing/decp-processing-exec-prod",
            "branch": "main",
            "dist_dir": "/srv/shared/decp/prod/dist",
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/decp_processing.py:decp_processing",
    ).deploy(
        name="decp-processing-exec-prod",
        description="Exécution de decp_processing sur l'instance Scaleway éphémère de prod.",
        work_pool_name="scaleway-prod",
        ignore_warnings=True,
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_USE_CACHE": "True",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/launcher.py:run_on_scaleway",
    ).deploy(
        name="decp-processing-dev",
        description="Déploiement de la branche dev, sur une instance Scaleway éphémère.",
        work_pool_name="local",
        ignore_warnings=True,
        parameters={
            "env": "dev",
            "deployment": "decp-processing/decp-processing-exec-dev",
            "branch": "dev",
            "dist_dir": "/srv/shared/decp/dev/dist",
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git",
            branch="dev",
        ),
        entrypoint="src/flows/decp_processing.py:decp_processing",
    ).deploy(
        name="decp-processing-exec-dev",
        description="Exécution de decp_processing sur l'instance Scaleway éphémère de dev.",
        work_pool_name="scaleway-dev",
        ignore_warnings=True,
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "False",
                "DECP_USE_CACHE": "False",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/launcher.py:run_on_scaleway",
    ).deploy(
        name="sirene-preprocess",
        description="Préparation des données SIRENE. Tous les mois, le 3, sur une instance Scaleway éphémère.",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 1 3 * *",
        parameters={
            "env": "prod",
            "deployment": "sirene-preprocess/sirene-preprocess-exec-prod",
            "branch": "main",
            "dist_dir": None,
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/sirene_preprocess.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess-exec-prod",
        description="Prétraitement SIRENE sur l'instance Scaleway éphémère de prod.",
        work_pool_name="scaleway-prod",
        ignore_warnings=True,
        job_variables={
            "env": {
                "PREFECT_TASKS_REFRESH_CACHE": "False",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/launcher.py:run_on_scaleway",
    ).deploy(
        name="sirene-preprocess-dev",
        description="Préparation des données SIRENE, sur une instance Scaleway éphémère.",
        work_pool_name="local",
        ignore_warnings=True,
        parameters={
            "env": "dev",
            "deployment": "sirene-preprocess/sirene-preprocess-exec-dev",
            "branch": "dev",
            "dist_dir": None,
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="dev"
        ),
        entrypoint="src/flows/sirene_preprocess.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess-exec-dev",
        description="Prétraitement SIRENE sur l'instance Scaleway éphémère de dev.",
        work_pool_name="scaleway-dev",
        ignore_warnings=True,
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudray/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/scrap.py:scrap",
    ).deploy(
        name="scrap-marches-securises",
        description="Scraping des données de marches-securises.fr.",
        ignore_warnings=True,
        work_pool_name="local",
        cron="0 0 * * 1-5",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "False",
                "SCRAPING_MODE": "month",
                "SCRAPING_TARGET": "marches-securises.fr",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/scrap.py:scrap",
    ).deploy(
        name="scrap-aws",
        description="Scraping des données de marches-publics.info.",
        ignore_warnings=True,
        work_pool_name="local",
        cron="0 0 * * 1-5",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "False",
                "SCRAPING_MODE": "month",
                "SCRAPING_TARGET": "aws",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/scrap.py:scrap",
    ).deploy(
        name="scrap-dume",
        description="Scraping des données de l'API DUME.",
        ignore_warnings=True,
        work_pool_name="local",
        cron="0 0 * * 2",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "False",
                "SCRAPING_MODE": "month",
                "SCRAPING_TARGET": "dume",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/scrap.py:scrap",
    ).deploy(
        name="scrap",
        description="Scraping des données.",
        ignore_warnings=True,
        work_pool_name="local",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "True",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="dev"
        ),
        entrypoint="src/flows/scrap.py:scrap",
    ).deploy(
        name="scrap-dev",
        description="Scraping des données.",
        ignore_warnings=True,
        work_pool_name="local",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/dev/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "True",
            }
        },
    )

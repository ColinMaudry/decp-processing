from prefect import flow
from prefect.runner.storage import GitRepository

# Rappel : pour déployer, PREFECT_API_URL doit être non null, sinon on déploie
# sur une instance éphémère.

if __name__ == "__main__":
    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows/decp_processing.py:decp_processing",
    ).deploy(
        name="decp-processing",
        description="Tous les jours du lundi au vendredi à 6h00",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 6 * * 1-5",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "False",
                "EXCLUDED_RESOURCES": "501b5201-bc83-4b59-b335-449b34043446,bd33e98f-f8e3-49ba-9f26-51c95fe57234",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git",
            branch="dev",
        ),
        entrypoint="src/flows/decp_processing.py:decp_processing",
    ).deploy(
        name="decp-processing-dev",
        description="Déploiement de la branche dev.",
        work_pool_name="local",
        ignore_warnings=True,
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "False",
                "DECP_DIST_DIR": "/srv/shared/decp/dev/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "True",
            }
        },
    )

    flow.from_source(
        source="https://github.com/ColinMaudry/decp-processing.git",
        entrypoint="src/flows/sirene_preprocess.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess",
        description="Préparation des données SIRENE. Tous les mois, le 3",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 1 3 * *",
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "True",
                "DECP_DIST_DIR": "/srv/shared/decp/prod/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "False",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="dev"
        ),
        entrypoint="src/flows/sirene_preprocess.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess-dev",
        description="Préparation des données SIRENE.",
        work_pool_name="local",
        ignore_warnings=True,
        job_variables={
            "env": {
                "DECP_PROCESSING_PUBLISH": "False",
                "DECP_DIST_DIR": "/srv/shared/decp/dev/dist",
                "PREFECT_TASKS_REFRESH_CACHE": "True",
            }
        },
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
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

from dotenv import dotenv_values
from prefect import flow
from prefect.runner.storage import GitRepository

if __name__ == "__main__":
    env = dotenv_values()
    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git", branch="main"
        ),
        entrypoint="src/flows.py:decp_processing",
    ).deploy(
        name="decp-processing",
        description="Tous les jours du lundi au vendredi à 6h00",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 6 * * 1-5",
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing.git",
            branch="dev",
        ),
        entrypoint="src/flows.py:decp_processing",
    ).deploy(
        name="decp-processing-dev",
        description="Déploiement de la branche dev.",
        work_pool_name="local",
        ignore_warnings=True,
    )

    flow.from_source(
        source="https://github.com/ColinMaudry/decp-processing.git",
        entrypoint="src/flows.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess",
        description="Préparation des données SIRENE. Tous les mois, le 3",
        work_pool_name="local",
        ignore_warnings=True,
        cron="0 1 3 * *",
    )

    flow.from_source(
        source=GitRepository(
            url="https://github.com/ColinMaudry/decp-processing/tree/dev", branch="dev"
        ),
        entrypoint="src/flows.py:sirene_preprocess",
    ).deploy(
        name="sirene-preprocess-dev",
        description="Préparation des données SIRENE.",
        work_pool_name="local",
        ignore_warnings=True,
    )

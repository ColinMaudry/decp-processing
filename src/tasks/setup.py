import os
import shutil

from prefect import task
from prefect.artifacts import create_table_artifact

from config import SIRENE_DATA_DIR


@task()
def initialization():
    # git pull
    # print("Récupération du code (pull)...")
    # command = "git pull origin main"
    # subprocess.run(command.split(" "))
    pass


def create_artifact(
    data,
    key: str,
    description: str = None,
):
    if data is list:
        create_table_artifact(key=key, table=data, description=description)


@task
def create_sirene_data_dir():
    os.makedirs(SIRENE_DATA_DIR, exist_ok=True)


# Si une tâche postérieure échoue dans le même flow que create_sirene_data_dir(), le dossier est supprimé
# Ainsi on garantie que si le dossier est présent, c'est que le flow (sirene_preprocess) est allé au bout
# https://docs.prefect.io/v3/advanced/transactions
@create_sirene_data_dir.on_rollback
def remove_sirene_data_dir(transaction):
    shutil.rmtree(SIRENE_DATA_DIR)

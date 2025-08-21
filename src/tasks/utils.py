import shutil
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from prefect import task
from prefect.artifacts import create_table_artifact

from config import (
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    PREFECT_LOCAL_STORAGE_PATH,
    SIRENE_DATA_DIR,
)


def create_artifact(
    data,
    key: str,
    description: str = None,
):
    if data is list:
        create_table_artifact(key=key, table=data, description=description)


@task
def create_sirene_data_dir():
    SIRENE_DATA_DIR.mkdir(exist_ok=True, parents=True)


# Si une tâche postérieure échoue dans le même flow que create_sirene_data_dir(), le dossier est supprimé
# Ainsi on garantie que si le dossier est présent, c'est que le flow (sirene_preprocess) est allé au bout
# https://docs.prefect.io/v3/advanced/transactions
@create_sirene_data_dir.on_rollback
def remove_sirene_data_dir(transaction):
    shutil.rmtree(SIRENE_DATA_DIR)


#
# CACHE
#


def get_clean_cache_key(context, parameters) -> str:
    resource = parameters["resource"]

    # On utilise le hash sha1 de la ressource, généré par data.gouv.fr, comme clé de cache
    return resource["checksum"]


@task
def remove_unused_cache(
    cache_dir: Path = PREFECT_LOCAL_STORAGE_PATH,
    cache_expiration_time_hours: int = CACHE_EXPIRATION_TIME_HOURS,
):
    now = time.time()
    age_limit = cache_expiration_time_hours * 3600  # seconds
    if cache_dir.exists():
        for file in cache_dir.rglob("*"):
            if file.is_file():
                if now - file.stat().st_atime > age_limit:
                    print(f"Deleting cache file: {file}")
                    file.unlink()


#
# STATS
#


def generate_stats(df: pl.DataFrame):
    now = datetime.now()
    df_uid: pl.DataFrame = df.select(
        "uid",
        "acheteur_id",
        "datePublicationDonnees",
        "dateNotification",
        "montant",
        "sourceOpenData",
    ).unique(subset=["uid"])

    stats = {
        "datetime": now.isoformat()[:-7],  # jusqu'aux secondes
        "date": DATE_NOW,
        "resources": df_uid["sourceOpenData"].unique().to_list(),
        "nb_lignes": df.height,
        "colonnes_triées": sorted(df.columns),
        "nb_colonnes": len(df.columns),
        "nb_marches": df_uid.height,
        "nb_acheteurs_uniques": df_uid.select("acheteur_id").unique().height
        - 1,  # -1 pour ne pas compter la valeur "acheteur vide"
        "nb_titulaires_uniques": df.select("titulaire_id", "titulaire_typeIdentifiant")
        .unique()
        .height
        - 1,  # -1 pour ne pas compter la valeur "titulaire vide"
    }

    for year in range(2018, int(DATE_NOW[0:4]) + 1):
        stats[f"{str(year)}_nb_publications_marchés"] = df_uid.filter(
            pl.col("datePublicationDonnees").dt.year() == year
        ).height

        df_date_notification = df_uid.filter(
            pl.col("dateNotification").dt.year() == year
        )
        stats[f"{str(year)}_nb_notifications_marchés"] = df_date_notification.height

        if df_date_notification.height > 0:
            stats[f"{str(year)}_somme_montant_marchés_notifiés"] = (
                df_date_notification.group_by("montant").sum()["montant"][0]
            )
            stats[f"{str(year)}_médiane_montant_marchés_notifiés"] = (
                df_date_notification.group_by("montant").median()["montant"][0]
            )
        else:
            stats[f"{str(year)}_somme_montant_marchés_notifiés"] = ""
            stats[f"{str(year)}_médiane_montant_marchés_notifiés"] = ""

    # Stock les statistiques dans prefect
    create_table_artifact(
        table=[stats],
        key="stats-marches-publics",
        description=f"Statistiques sur les marchés publics agrégés ({DATE_NOW})",
    )

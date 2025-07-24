from datetime import datetime

import polars as pl

from config import DATE_NOW
from tasks.setup import create_table_artifact


def list_data_issues(df: pl.LazyFrame):
    df = df.collect()

    # Dates impossibles

    date_columns = [
        "dateNotification",
        "dateNotificationActeSousTraitance",
        "dateNotificationModificationModification",
        "dateNotificationModificationSousTraitanceModificationActeSousTraitance",
        "datePublicationDonnees",
        "datePublicationDonneesActeSousTraitance",
        "datePublicationDonneesModificationActeSousTraitance",
        "datePublicationDonneesModificationModification",
    ]

    for column in date_columns:
        print(
            "Dates impossibles dans la colonne ",
            column,
            ":",
            df.filter(
                (pl.col(column) < pl.date(2015, 1, 1))
                | (pl.col(column) > datetime.now())
            ).height,
        )


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

import json
import logging
import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from prefect import task
from prefect.artifacts import create_table_artifact
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger

from src.config import (
    ALL_CONFIG,
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    DIST_DIR,
    LOG_LEVEL,
    RESOURCE_CACHE_DIR,
    SIRENE_DATA_DIR,
    TRACKED_DATASETS,
    DecpFormat,
)


def stream_replace_bytestring(iterator, old_bytestring: bytes, new_bytestring: bytes):
    """
    Remplacement de texte encodé avec regex, avec prise en compte des bords de "chunk (merci Euria la LLM d'Infomaniak)
    """
    buffer = b""
    length_to_replace = len(old_bytestring)

    for chunk in iterator:
        buffer += chunk
        buffer = re.sub(old_bytestring, new_bytestring, buffer)
        # On garde un bout de quelques caractères au cas où le pattern est coupé entre les deux chunks
        safe_end = len(buffer) - length_to_replace + 1

        if safe_end > 0:
            to_process, buffer = buffer[:safe_end], buffer[safe_end:]
            chunk = to_process.replace(old_bytestring, new_bytestring)
            yield chunk

    if buffer:
        buffer = re.sub(old_bytestring, new_bytestring, buffer)
        yield buffer


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


def remove_unused_cache(
    cache_dir: Path = RESOURCE_CACHE_DIR,
    cache_expiration_time_hours: int = CACHE_EXPIRATION_TIME_HOURS,
):
    logger = get_logger(level=LOG_LEVEL)

    now = time.time()
    age_limit = cache_expiration_time_hours * 3600  # seconds
    deleted_files = []
    if cache_dir.exists():
        for file in cache_dir.rglob("*"):
            if file.is_file():
                if now - file.stat().st_atime > age_limit:
                    logger.debug(f"Suppression du fichier de cache: {file}")
                    deleted_files.append(file)
                    file.unlink()
        logger.info(f"-> {len(deleted_files)} fichiers de cache supprimés")


#
# STATS
#


# Statistiques pour une ressource
def gen_artifact_row(
    file_info: dict,
    lf: pl.LazyFrame,
    url: str,
    fields: set[str],
    decp_format: DecpFormat,
):
    artifact_row = {
        # file and schema metadata
        "open_data_dataset_id": file_info["dataset_id"],
        "open_data_dataset_name": file_info["dataset_name"],
        "download_date": DATE_NOW,
        "data_fields": sorted(list(fields)),
        "data_fields_number": len(fields),
        "schema_label": decp_format.label,
        "row_number": lf.select(pl.len()).collect().item(),
        # data.gouv.fr metadata
        "open_data_filename": file_info["ori_filename"],
        "open_data_id": file_info["id"],
        "sha1": file_info["checksum"],
        "created_at": file_info["created_at"],
        "last_modified": file_info["last_modified"],
        "filesize": file_info["filesize"],
        "views": file_info["views"],
        "url": url,
    }

    return artifact_row


# Statistiques pour toutes les données collectées ce jour
def generate_stats(lf: pl.LazyFrame):
    now = datetime.now()

    lf = lf.with_columns(
        pl.col("dateNotification").dt.year().alias("anneeNotification")
    )
    lf = lf.with_columns(
        pl.col("datePublicationDonnees").dt.year().alias("anneePublicationDonnees")
    )
    lf_uid = (
        lf.select(
            "uid",
            "acheteur_id",
            "dateNotification",
            "datePublicationDonnees",
            "montant",
            "donneesActuelles",
            "sourceDataset",
            "sourceFile",
            "anneeNotification",
            "anneePublicationDonnees",
        )
        .filter(pl.col("donneesActuelles"))
        .unique(subset=["uid"])
    )

    # Statistiques sur les sources de données (statistiques.csv)
    # generate_public_source_stats does aggregations. Let's make it lazy too.
    generate_public_source_stats(lf_uid)

    # Collect only the necessary aggregates for the main stats
    # We need to compute several things. It might be efficient to do one big aggregation or several small collects.

    # 1. Resources and Sources
    resources = (
        lf_uid.select("sourceFile")
        .unique()
        .collect()
        .get_column("sourceFile")
        .to_list()
    )
    sources = (
        lf_uid.select("sourceDataset")
        .unique()
        .collect()
        .get_column("sourceDataset")
        .to_list()
    )

    # 2. Counts
    nb_lignes = lf.select(pl.len()).collect().item()
    nb_marches = lf_uid.select(pl.len()).collect().item()

    # 3. Unique counts (approximate or exact)
    nb_acheteurs_uniques = (
        lf_uid.select("acheteur_id").unique().select(pl.len()).collect().item() - 1
    )
    nb_titulaires_uniques = (
        lf.select("titulaire_id", "titulaire_typeIdentifiant")
        .unique()
        .select(pl.len())
        .collect()
        .item()
        - 1
    )

    # 4. Columns
    columns = lf.collect_schema().names()

    stats = {
        "datetime": now.isoformat()[:-7],  # jusqu'aux secondes
        "date": DATE_NOW,
        "nb_resources": len(resources),
        "sources": sources,
        "nb_lignes": nb_lignes,
        "colonnes_triees": sorted(columns),
        "nb_colonnes": len(columns),
        "nb_marches": nb_marches,
        "nb_acheteurs_uniques": nb_acheteurs_uniques,
        "nb_titulaires_uniques": nb_titulaires_uniques,
    }

    # 5. Yearly stats
    # We can do this with a group_by and then iterate over the result

    # Aggregations for publications per year
    pub_stats = (
        lf_uid.group_by("anneePublicationDonnees")
        .agg(pl.len().alias("count"))
        .collect()
    )

    # Aggregations for notifications per year (count, sum, median)
    notif_stats = (
        lf_uid.group_by("anneeNotification")
        .agg(
            pl.len().alias("count"),
            pl.sum("montant").alias("sum_montant"),
            pl.median("montant").alias("median_montant"),
        )
        .collect()
    )

    for year in range(2018, int(DATE_NOW[0:4]) + 1):
        stats[str(year)] = stats_year = {}

        # Publications
        pub_count = (
            pub_stats.filter(pl.col("anneePublicationDonnees") == year)
            .select("count")
            .item()
            if not pub_stats.filter(
                pl.col("anneePublicationDonnees") == year
            ).is_empty()
            else 0
        )
        stats_year["nb_publications_marches"] = pub_count

        # Notifications
        df_year_stats = notif_stats.filter(pl.col("anneeNotification") == year)
        if not df_year_stats.is_empty():
            stats_year["nb_notifications_marches"] = df_year_stats.select(
                "count"
            ).item()
            stats_year["somme_montant_marches_notifies"] = int(
                df_year_stats.select("sum_montant").item() or 0
            )
            stats_year["mediane_montant_marches_notifies"] = int(
                df_year_stats.select("median_montant").item() or 0
            )
            stats_year["nb_acheteurs_uniques"] = (
                lf_uid.filter(pl.col("anneeNotification") == year)
                .select("acheteur_id")
                .unique()
                .collect(engine="streaming")
                .height
            )
            stats_year["nb_titulaires_uniques"] = (
                lf.filter(pl.col("anneeNotification") == year)
                .select("titulaire_id")
                .unique()
                .collect(engine="streaming")
                .height
            )

        else:
            stats_year["nb_notifications_marches"] = 0
            stats_year["somme_montant_marchés_notifies"] = ""
            stats_year["mediane_montant_marchés_notifies"] = ""

    # Stock les statistiques dans prefect
    create_table_artifact(
        table=[stats],
        key="stats-marches-publics",
        description=f"Statistiques sur les marchés publics agrégés ({DATE_NOW})",
    )

    # Création d'un JSON pour publication sur data.gouv.fr
    with open(DIST_DIR / "statistiques_marches.json", "w") as f:
        json.dump(stats, f, indent=4)


def generate_public_source_stats(lf_uid: pl.LazyFrame) -> None:
    logger = get_logger(level=LOG_LEVEL)

    logger.info("Génération des statistiques sur les sources de données...")
    lf_uid = lf_uid.select("uid", "acheteur_id", "sourceDataset")

    # We need to collect these intermediate aggregations to join them with the sources dataframe (which is small)
    df_acheteurs = (
        lf_uid.select("acheteur_id", "sourceDataset")
        .unique()
        .group_by("sourceDataset")
        .len()
        .collect()
    )
    df_acheteurs = df_acheteurs.rename({"len": "nb_acheteurs"})

    # group + count
    df_uid_agg = (
        lf_uid.select("uid", "sourceDataset")
        .unique()
        .group_by("sourceDataset")
        .len()
        .collect()
    )
    df_uid_agg = df_uid_agg.rename({"len": "nb_marchés"}).sort(
        by="nb_marchés", descending=True
    )

    # lecture des sources en df
    df_sources: pl.DataFrame = pl.DataFrame(TRACKED_DATASETS)
    # si c'est les données de test
    if "resources" in df_sources.columns:
        df_sources = df_sources.drop("resources")

    # petites modifications
    df_sources = df_sources.with_columns(
        (pl.lit("https://www.data.gouv.fr/datasets/") + pl.col("id")).alias("url")
    )
    df_sources = df_sources.rename(
        {"name": "nom", "owner_org_name": "organisation"}
    ).drop("id")

    # ajout données count
    df_sources = df_sources.join(
        df_acheteurs,
        left_on="code",
        right_on="sourceDataset",
        how="full",
        coalesce=True,
    )
    df_sources = df_sources.join(
        df_uid_agg, left_on="code", right_on="sourceDataset", how="full", coalesce=True
    )

    # ordre des colonnes
    df_sources = df_sources.select(
        "nom", "organisation", "url", "nb_marchés", "nb_acheteurs", "code"
    )

    # jointure avec la matrice de présence
    df_matrice = pl.read_parquet(
        DIST_DIR / "statistiques_doublons_sources.parquet",
        columns=["sourceDataset", "unique"],
    )
    df_sources = df_sources.join(df_matrice, left_on="code", right_on="sourceDataset")

    # application des métadonnées decp_minef aux codes decp_minef_*
    df_sources = (
        df_sources.sort(by="code").with_columns(
            pl.col("nom", "organisation", "url").fill_null(strategy="forward")
        )
    ).filter(pl.col("code") != "decp_minef")

    # remplacement des null par zéros
    df_sources = df_sources.fill_null(0)

    # tri par nombre de marchés
    df_sources = df_sources.sort(by="nb_marchés", descending=True, nulls_last=True)

    # dump CSV dans dist
    df_sources.write_csv(DIST_DIR / "statistiques_sources.csv")


def full_resource_name(r: dict):
    """Retourne le nom du fichier de la ressource et le nom du dataset."""
    return f"{r['ori_filename']} ({r['dataset_name']})"


def check_parquet_file(path) -> bool:
    try:
        lf = pl.scan_parquet(path)
        height = lf.select(pl.count()).collect().item()
        result = height > 0
        del lf
        return result
    except (FileNotFoundError, pl.exceptions.ComputeError):
        return False


def print_all_config():
    logger = get_logger(level=LOG_LEVEL)
    all_config = ALL_CONFIG

    msg = ""
    for k, v in sorted(all_config.items()):
        msg += f"\n{k}: {v}"
    logger.info(msg)


def get_logger(level: str) -> logging.Logger:
    try:
        logger = get_run_logger()
        logger.setLevel(level)
        return logger
    except MissingContextError:
        return logging.Logger(name="Fallback logger", level=level)


def calculate_duplicates_across_source(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Cette fonction crée une matrice qui indique, pour chaque code de source
    (exemples : pes_marche_2024, xmarches) le pourcentage d'uids qui lui sont uniques
    (présentes dans aucune autre source) et le pourcentage d'uids présentes également
    dans chaque autre source.

    Fonction générée par la LLM Geminio 3 pro et testée par un humain.
    :param lf:
    :return:
    """

    # 1. Start Lazy and deduplicate (UID + Source must be unique)
    lf = lf.unique(["uid", "sourceDataset"])

    # 2. Create the Membership Matrix
    # This creates a table: uid | dataset1 (bool) | dataset2 (bool) | ...
    membership = (
        lf.with_columns(pl.lit(True).alias("exists"))
        .collect()  # Pivot is not yet fully streaming in Lazy, so we collect here
        .pivot(on="sourceDataset", index="uid", values="exists")
        .fill_null(False)
    )

    # Get the names of all dataset columns
    source_cols = [c for c in membership.columns if c != "uid"]

    # 3. Calculate "Row Sum" to find purely unique UIDs
    # (A UID is unique if it appears in exactly 1 column)
    membership = membership.with_columns(
        pl.sum_horizontal(pl.col(source_cols).cast(pl.Int8)).alias("appearance_count")
    )

    results = []
    for source in source_cols:
        # 1. Total UIDs in this source
        # (Summing a boolean column treats True as 1)
        total_in_source = membership.select(pl.col(source).sum()).item()

        if total_in_source == 0:
            continue

        # 2. Unique count
        # We use a logical AND then sum the result
        unique_count = membership.select(
            (pl.col(source) & (pl.col("appearance_count") == 1)).sum()
        ).item()

        row_stats = {"sourceDataset": source, "unique": unique_count / total_in_source}

        # 3. Intersections
        for other in source_cols:
            if source == other:
                continue

            # Intersection: where both columns are True
            intersect_count = membership.select(
                (pl.col(source) & pl.col(other)).sum()
            ).item()

            row_stats[other] = intersect_count / total_in_source

        results.append(row_stats)

    result = pl.DataFrame(results)
    result.write_parquet(DIST_DIR / "statistiques_doublons_sources.parquet")
    # Le return est pour tester la fonction
    return result

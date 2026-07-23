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
    BASE_DF_COLUMNS,
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    DIST_DIR,
    RESOURCE_CACHE_DIR,
    SIRENE_DATA_DIR,
    TRACKED_DATASETS,
    DecpFormat,
    logger,
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


def log_column_stats(lf: pl.LazyFrame, nb_lignes: int) -> None:
    """Log, pour chaque colonne, son nombre de valeurs distinctes (hors null) et son % de valeurs nulles."""
    columns = lf.collect_schema().names()

    exprs = []
    for col in columns:
        exprs.append(pl.col(col).null_count().alias(f"{col}__null_count"))
        exprs.append(pl.col(col).n_unique().alias(f"{col}__n_unique"))

    stats_row = lf.select(exprs).collect().row(0, named=True)

    header = f"{'colonne':<40}{'valeurs distinctes':>20}{'% null':>10}"
    lines = [header, "-" * len(header)]

    for col in sorted(columns):
        null_count = stats_row[f"{col}__null_count"]
        n_unique = stats_row[f"{col}__n_unique"]
        n_unique_non_null = n_unique - 1 if null_count > 0 else n_unique
        pct_null = (null_count / nb_lignes * 100) if nb_lignes > 0 else 0.0
        lines.append(f"{col:<40}{n_unique_non_null:>20}{pct_null:>9.2f}%")

    logger.info(
        "Statistiques par colonne (valeurs distinctes / % null) :\n" + "\n".join(lines)
    )


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

    generate_public_source_stats(lf_uid)

    logger.info("Création de l'artefact et du JSON de statistiques...")

    # Collect lf_uid once — avoids re-scanning parquet for every subsequent query
    df_uid = lf_uid.collect()

    # 1. Resources and Sources
    resources = df_uid["sourceFile"].unique().to_list()
    sources = df_uid["sourceDataset"].unique().to_list()

    # 2. Counts
    nb_lignes = lf.select(pl.len()).collect().item()
    # Uniquement les colonnes du schéma publié, pas les colonnes internes
    # (ex. anneeNotification, anneePublicationDonnees) ajoutées ci-dessus pour
    # les besoins de generate_stats.
    log_column_stats(lf.select(BASE_DF_COLUMNS), nb_lignes)
    nb_marches = len(df_uid)

    # 3. Unique counts
    nb_acheteurs_uniques = df_uid["acheteur_id"].n_unique() - 1
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

    # 5. Yearly stats — pre-aggregate in one pass to avoid per-year filter+collect in the loop
    pub_stats = df_uid.group_by("anneePublicationDonnees").agg(pl.len().alias("count"))

    notif_stats = df_uid.group_by("anneeNotification").agg(
        pl.len().alias("count"),
        pl.sum("montant").alias("sum_montant"),
        pl.median("montant").alias("median_montant"),
        pl.col("acheteur_id").n_unique().alias("nb_acheteurs_uniques"),
    )

    titulaire_year_stats = (
        lf.group_by("anneeNotification")
        .agg(pl.col("titulaire_id").n_unique().alias("nb_titulaires_uniques"))
        .collect()
    )

    for year in range(2018, int(DATE_NOW[0:4]) + 1):
        stats[str(year)] = stats_year = {}

        # Publications
        pub_rows = pub_stats.filter(pl.col("anneePublicationDonnees") == year)
        stats_year["nb_publications_marches"] = (
            pub_rows.select("count").item() if not pub_rows.is_empty() else 0
        )

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
            stats_year["nb_acheteurs_uniques"] = df_year_stats.select(
                "nb_acheteurs_uniques"
            ).item()
            tit_rows = titulaire_year_stats.filter(pl.col("anneeNotification") == year)
            stats_year["nb_titulaires_uniques"] = (
                tit_rows.select("nb_titulaires_uniques").item()
                if not tit_rows.is_empty()
                else 0
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

    # 3. "Row Sum" : un UID est unique s'il n'apparaît que dans une seule source
    membership = membership.with_columns(
        pl.sum_horizontal(pl.col(source_cols).cast(pl.Int8)).alias("appearance_count")
    )

    # 4. Toutes les agrégations (total par source, comptes uniques, intersections par
    # paire) sont calculées en UNE seule passe. L'ancienne version enchaînait
    # O(S²) appels .select(...).item() (une exécution Polars complète par paire de
    # sources), coûteux dès que le nombre de sources grandit.
    agg_exprs = []
    for i, source in enumerate(source_cols):
        agg_exprs.append(pl.col(source).sum().alias(f"total_{i}"))
        agg_exprs.append(
            (pl.col(source) & (pl.col("appearance_count") == 1))
            .sum()
            .alias(f"uniq_{i}")
        )
        for j, other in enumerate(source_cols):
            if i != j:
                agg_exprs.append(
                    (pl.col(source) & pl.col(other)).sum().alias(f"inter_{i}_{j}")
                )

    aggs = membership.select(agg_exprs).row(0, named=True)

    # 5. Reconstruction du tableau de résultats à partir des scalaires agrégés
    results = []
    for i, source in enumerate(source_cols):
        total_in_source = aggs[f"total_{i}"]
        if total_in_source == 0:
            continue

        row_stats = {
            "sourceDataset": source,
            "unique": aggs[f"uniq_{i}"] / total_in_source,
        }
        for j, other in enumerate(source_cols):
            if i == j:
                continue
            row_stats[other] = aggs[f"inter_{i}_{j}"] / total_in_source

        results.append(row_stats)

    result = pl.DataFrame(results)
    result.write_parquet(DIST_DIR / "statistiques_doublons_sources.parquet")
    # Le return est pour tester la fonction
    return result

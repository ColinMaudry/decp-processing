import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from prefect import task
from prefect.artifacts import create_table_artifact

from src.config import (
    CACHE_EXPIRATION_TIME_HOURS,
    DATE_NOW,
    DIST_DIR,
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
    now = time.time()
    age_limit = cache_expiration_time_hours * 3600  # seconds
    deleted_files = []
    if cache_dir.exists():
        for file in cache_dir.rglob("*"):
            if file.is_file():
                if now - file.stat().st_atime > age_limit:
                    print(f"Suppression du fichier de cache: {file}")
                    deleted_files.append(file)
                    file.unlink()
        print(f"-> {len(deleted_files)} fichiers supprimés")


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
        "resources": resources,
        "nb_resources": len(resources),
        "sources": sources,
        "nb_lignes": nb_lignes,
        "colonnes_triées": sorted(columns),
        "nb_colonnes": len(columns),
        "nb_marches": nb_marches,
        "nb_acheteurs_uniques": nb_acheteurs_uniques,
        "nb_titulaires_uniques": nb_titulaires_uniques,
    }

    # 5. Yearly stats
    # We can do this with a group_by and then iterate over the result

    # Aggregations for publications per year
    pub_stats = (
        lf_uid.with_columns(pl.col("datePublicationDonnees").dt.year().alias("year"))
        .group_by("year")
        .agg(pl.len().alias("count"))
        .collect()
    )

    # Aggregations for notifications per year (count, sum, median)
    notif_stats = (
        lf_uid.with_columns(pl.col("dateNotification").dt.year().alias("year"))
        .group_by("year")
        .agg(
            pl.len().alias("count"),
            pl.sum("montant").alias("sum_montant"),
            pl.median("montant").alias("median_montant"),
        )
        .collect()
    )

    for year in range(2018, int(DATE_NOW[0:4]) + 1):
        # Publications
        pub_count = (
            pub_stats.filter(pl.col("year") == year).select("count").item()
            if not pub_stats.filter(pl.col("year") == year).is_empty()
            else 0
        )
        stats[f"{str(year)}_nb_publications_marchés"] = pub_count

        # Notifications
        year_stats = notif_stats.filter(pl.col("year") == year)
        if not year_stats.is_empty():
            stats[f"{str(year)}_nb_notifications_marchés"] = year_stats.select(
                "count"
            ).item()
            stats[f"{str(year)}_somme_montant_marchés_notifiés"] = int(
                year_stats.select("sum_montant").item() or 0
            )
            stats[f"{str(year)}_médiane_montant_marchés_notifiés"] = int(
                year_stats.select("median_montant").item() or 0
            )
        else:
            stats[f"{str(year)}_nb_notifications_marchés"] = 0
            stats[f"{str(year)}_somme_montant_marchés_notifiés"] = ""
            stats[f"{str(year)}_médiane_montant_marchés_notifiés"] = ""

    # Stock les statistiques dans prefect
    create_table_artifact(
        table=[stats],
        key="stats-marches-publics",
        description=f"Statistiques sur les marchés publics agrégés ({DATE_NOW})",
    )


def generate_public_source_stats(lf_uid: pl.LazyFrame) -> None:
    print("Génération des statistiques sur les sources de données...")
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
    df_sources.write_csv(DIST_DIR / "statistiques.csv")


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

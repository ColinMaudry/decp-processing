import json
import sqlite3
from collections import ChainMap
from itertools import groupby
from operator import itemgetter
from pathlib import Path

import polars as pl
from polars import selectors as cs
from prefect import task

from src.config import DIST_DIR, POSTGRESQL_DB_URI, REFERENCE_DIR


def save_to_files(df: pl.DataFrame, path: Path, file_format=None):
    if file_format is None:
        file_format = ["csv", "parquet"]
    if "parquet" in file_format:
        tmp_path = path.with_suffix(".parquet.tmp")
        df.write_parquet(tmp_path)
        tmp_path.rename(path.with_suffix(".parquet"))
    if "csv" in file_format:
        df.write_csv(f"{path}.csv")


def sink_to_files(
    lf: pl.LazyFrame, path: str | Path, file_format=None, compression: str = "zstd"
):
    path = Path(path)
    if file_format is None:
        file_format = ["csv", "parquet"]

    if "parquet" in file_format:
        # Write to a temporary file first to avoid read-write conflicts
        tmp_path = path.with_suffix(".parquet.tmp")

        lf.sink_parquet(tmp_path, compression=compression, engine="streaming")

        if tmp_path.exists():
            tmp_path.rename(path.with_suffix(".parquet"))

        # Utilisation du parquet plutôt que de relaculer le plan de requête du LazyFrame
        if "csv" in file_format:
            pl.scan_parquet(f"{path}.parquet").sink_csv(
                f"{path}.csv", engine="streaming"
            )

    elif "csv" in file_format:
        # Fallback if only CSV is requested
        lf.sink_csv(f"{path}.csv", engine="streaming")

    # Si ça peut réduire un peu l'empreinte mémoire
    del lf


def save_to_postgres(df: pl.DataFrame, table_name: str):
    df.write_database(
        table_name=table_name,
        connection=POSTGRESQL_DB_URI,
        engine="sqlalchemy",
        engine_options={},
        if_table_exists="replace",
    )


def save_to_sqlite(lf: pl.LazyFrame, database: str, table_name: str, primary_key: str):
    # Création de la table, avec les définitions de colonnes et de la ou des clés primaires
    column_definitions = []
    schema = lf.collect_schema()
    for column in schema.keys():
        if schema[column] in [pl.Int16, pl.Int64, pl.Boolean]:
            sql_type = "INTEGER"
        elif schema[column] in [pl.Float32, pl.Float64]:
            sql_type = "REAL"
        else:
            sql_type = "TEXT"
        column_definitions.append(f'"{column}" {sql_type}')

    if "." in primary_key and '"' not in primary_key:
        raise ValueError(
            f"Les noms de colonnes contenant un point doivent être entre guillemets : {primary_key}"
        )

    primary_key_definition = (
        f"PRIMARY KEY({primary_key})"  # Peut être une clé composite. Ex : id, type
    )
    create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_definitions)}, {primary_key_definition})'  # Add quotes

    # Éxecution de la requête
    connection = sqlite3.connect(DIST_DIR / f"{database}.sqlite")
    cursor = connection.cursor()
    # Important de "DROP TABLE IF EXISTS", le fichier sqlite de la veille pré-existera en général
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cursor.execute(create_table_sql)
    connection.commit()
    connection.close()

    # Batch size
    batch_size = 50000
    offset = 0
    while True:
        # Récupération du batch
        batch = lf.slice(offset, batch_size).collect(engine="streaming")

        # Fin de la boucle si plus de données
        if batch.height == 0:
            break

        # Écriture du batch dans SQLite
        batch.write_database(
            f'"{table_name}"',
            connection=f"sqlite:///{DIST_DIR}/{database}.sqlite",
            if_table_exists="append",
        )

        offset += batch_size


def save_to_databases(
    lf: pl.LazyFrame, database: str, table_name: str, primary_key: str
):
    save_to_sqlite(lf, database, table_name, primary_key)

    # Pas utilisé pour l'instant
    # if (
    #     POSTGRESQL_DB_URI != "postgresql://user:pass@server:port/database"
    #     and POSTGRESQL_DB_URI is not None
    # ):
    #     save_to_postgres(df, table_name)


def generate_final_schema(lf):
    """Création d'un TableSchema pour décrire les données publiées"""

    schema = lf.collect_schema()
    frictonless_schema = []

    polars_frictionless_mapping = {
        "String": "string",
        "Float32": "number",
        "Float64": "number",
        "Int16": "integer",
        "Boolean": "boolean",
        "Date": "date",
    }

    # conversion en dict sérialisable en JSON
    for col in schema.keys():
        polars_type = schema[col].__str__()
        frictonless_schema.append(
            {"name": col, "type": polars_frictionless_mapping[polars_type]}
        )

    # récupération de data/data_fields.json
    with open(REFERENCE_DIR / "schema_base.json", "r", encoding="utf-8") as file:
        base_json = json.load(file)

    # fusion des deux
    # https://www.paigeniedringhaus.com/blog/filter-merge-and-update-python-lists-based-on-object-attributes#merge-two-lists-together-by-matching-object-keys
    merged_fields = groupby(
        sorted(base_json["fields"] + frictonless_schema, key=itemgetter("name")),
        itemgetter("name"),
    )
    merged_schema = {"fields": [dict(ChainMap(*g)) for k, g in merged_fields]}

    # création de dist/schema.json
    with open(DIST_DIR / "schema.json", "w", encoding="utf-8") as file:
        json.dump(merged_schema, file, indent=4, ensure_ascii=False, sort_keys=False)


@task(log_prints=True)
def make_data_tables():
    """Tâches consacrées à la transformation des données dans un format relationnel (SQL)."""

    print("Création de la base données au format relationnel...")

    lf: pl.LazyFrame = pl.scan_parquet(DIST_DIR / "decp.parquet")

    print("Enregistrement des DECP (base DataFrame) dans les bases de données...")
    print(DIST_DIR / "decp.parquet")
    save_to_databases(
        lf,
        "decp",
        "data.gouv.fr.2022.clean",
        "uid, titulaire_id, titulaire_typeIdentifiant, modification_id",
    )

    print("Normalisation des tables...")
    normalize_tables(lf)


def normalize_tables(lf: pl.LazyFrame):
    # MARCHES

    lf_marches: pl.LazyFrame = lf.drop(cs.starts_with("titulaire", "acheteur"))
    lf_marches = lf_marches.unique(subset=["uid", "modification_id"]).sort(
        by="dateNotification", descending=True
    )
    save_to_databases(lf_marches, "decp", "marches", "uid, modification_id")

    # ACHETEURS

    lf_acheteurs: pl.LazyFrame = lf.select(cs.starts_with("acheteur"))
    lf_acheteurs = lf_acheteurs.rename(lambda name: name.removeprefix("acheteur_"))
    lf_acheteurs = lf_acheteurs.unique().sort(by="id")
    save_to_databases(lf_acheteurs, "decp", "acheteurs", "id")

    # TITULAIRES

    ## Table entreprises
    lf_titulaires: pl.LazyFrame = lf.select(cs.starts_with("titulaire"))

    ### On garde les champs id et typeIdentifiant en clé primaire composite
    lf_titulaires = lf_titulaires.rename(lambda name: name.removeprefix("titulaire_"))
    lf_titulaires = lf_titulaires.unique().sort(by=["id"])
    save_to_databases(lf_titulaires, "decp", "entreprises", "id, typeIdentifiant")
    del lf_titulaires

    ## Table marches_titulaires
    lf_marches_titulaires: pl.LazyFrame = lf.select(
        "uid", "modification_id", "titulaire_id", "titulaire_typeIdentifiant"
    )

    save_to_databases(
        lf_marches_titulaires,
        "decp",
        "marches_titulaires",
        '"uid", "modification_id", "titulaire_id", "titulaire_typeIdentifiant"',
    )

    ## Table marches_acheteurs
    lf_marches_acheteurs: pl.LazyFrame = lf.select(
        "uid", "modification_id", "acheteur_id"
    ).unique()

    save_to_databases(
        lf_marches_acheteurs,
        "decp",
        "marches_acheteurs",
        '"uid", "modification_id", "acheteur_id"',
    )

    # TODO ajouter les sous-traitants quand ils seront ajoutés aux données

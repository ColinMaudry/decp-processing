import json
import sqlite3
from collections import OrderedDict
from pathlib import Path

import polars as pl

from config import DATA_DIR, DIST_DIR, POSTGRESQL_DB_URI


def save_to_files(df: pl.DataFrame, path: str | Path, file_format=None):
    if file_format is None:
        file_format = ["csv", "parquet"]
    if "parquet" in file_format:
        df.write_parquet(f"{path}.parquet")
    if "csv" in file_format:
        df.write_csv(f"{path}.csv")


def sink_to_files(lf: pl.LazyFrame, path: str | Path, file_format=None):
    if file_format is None:
        file_format = ["csv", "parquet"]
    if "parquet" in file_format:
        lf.sink_parquet(f"{path}.parquet")
    if "csv" in file_format:
        lf.sink_csv(f"{path}.csv")


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


def generate_final_schema(df):
    final_schema = OrderedDict()

    # conversion en dict sérialisable en JSON
    for col in df.columns:
        final_schema[col] = {"datatype": df.schema[col].__str__()}

    # récupération de data/data_fields.json
    with open(DATA_DIR / "schema_base.json", "r", encoding="utf-8") as file:
        base_json = json.load(file, object_pairs_hook=OrderedDict)

    # fusion des deux
    for field in final_schema:
        if field not in base_json:
            print(field + " absent de schema_base.json !")
        else:
            merged = OrderedDict(base_json[field])  # Copy to preserve order
            merged.update(final_schema[field])  # Add/override with datatype
            final_schema[field] = merged

    # création de dist/schema.json
    with open(DIST_DIR / "schema.json", "w", encoding="utf-8") as file:
        json.dump(final_schema, file, indent=4, ensure_ascii=False, sort_keys=False)

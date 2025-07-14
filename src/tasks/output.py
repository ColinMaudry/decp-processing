import os
import shutil
import sqlite3
import uuid
from pathlib import Path

import polars as pl

from config import DIST_DIR


def polars_parquet_write(
    df: pl.DataFrame,
    path: str | Path,
    partition_keys: list = None,
    if_exists: str = "replace",
):
    """
    Compte tenu de la nature instable de la gestion des partitions parquet avec polars et l'absence de strategie replace/append, on créé un helper pour gérer l'écriture parquet
    --> https://docs.pola.rs/api/python/dev/reference/api/polars.DataFrame.write_parquet.html
    partition_by
        Column(s) to partition by. A partitioned dataset will be written if this is specified. This parameter is considered unstable and is subject to change.
    """

    def get_filename(base_path, if_exists):
        "Helper function qui gère les noms de fichiers et l'écrasement de données précédentes"
        if if_exists == "append":
            filename = f"part-{uuid.uuid4().hex}.parquet"
        elif if_exists == "replace":
            filename = "data.parquet"
            # S'il y a déjà des fichiers dans la destination, on les supprime
            shutil.rmtree(base_path, ignore_errors=True)
            os.makedirs(base_path, exist_ok=True)

        else:
            raise AttributeError(
                'if_exist doit prendre une des valeurs "append" ou "replace"'
            )

        return os.path.join(base_path, filename)

    if partition_keys:
        for group in df.partition_by(partition_keys):
            partition_vals = [f"{col}={group[col][0]}" for col in partition_keys]
            partition_path = os.path.join(path, *partition_vals)

            os.makedirs(partition_path, exist_ok=True)
            full_path = get_filename(partition_path, if_exists)

            group.write_parquet(full_path)

    else:
        full_path = get_filename(path, if_exists)
        df.write_parquet(f"{path}")


def save_to_files(
    df: pl.DataFrame, path: str | Path, file_format=None, partition_key: str = None
):
    if file_format is None:
        file_format = ["csv", "parquet"]
    if "csv" in file_format:
        df.write_csv(f"{path}.csv")
    if "parquet" in file_format:
        if partition_key:
            df.write_parquet(f"{path}", partition_by=partition_key)
        else:
            df.write_parquet(f"{path}.parquet")


def save_to_sqlite(df: pl.DataFrame, database: str, table_name: str, primary_key: str):
    # Création de la table, avec les définitions de colonnes et de la ou des clés primaires
    column_definitions = []
    for column_name, column_type in zip(df.columns, df.dtypes):
        sql_type = "TEXT"  # Default
        if column_type in [pl.Int16, pl.Int64, pl.Boolean]:
            sql_type = "INTEGER"
        elif column_type in [pl.Float32, pl.Float64]:
            sql_type = "REAL"
        column_definitions.append(f'"{column_name}" {sql_type}')

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

    df.write_database(
        f'"{table_name}"',
        f"sqlite:///{DIST_DIR}/{database}.sqlite",
        if_table_exists="append",
    )


def make_data_package():
    from frictionless import Package, Resource, steps

    common_steps = [
        steps.field_update(name="id", descriptor={"type": "string"}),
        steps.field_update(name="uid", descriptor={"type": "string"}),
        steps.field_update(name="acheteur_id", descriptor={"type": "string"}),
    ]

    outputs = [
        {
            "csv": str(DIST_DIR / "decp.csv"),
            "steps": common_steps
            + [
                steps.field_update(name="titulaire_id", descriptor={"type": "string"}),
            ],
        },
        {
            "csv": str(DIST_DIR / "decp-sans-titulaires.csv"),
            "steps": common_steps,
        },
        # {
        #     "csv": f"{DIST_DIR}/decp-titulaires.csv",
        #     "steps": common_steps
        #     + [
        #         steps.field_update(name="departement", descriptor={"type": "string"}),
        #         steps.field_update(name="titulaire_id", descriptor={"type": "string"}),
        #     ],
        # },
    ]

    resources = []

    for output in outputs:
        resource: Resource = Resource(path=output["csv"])

        # Cette méthode détecte les caractéristiques du CSV et tente de deviner les datatypes
        resources.append(Resource.transform(steps=output["steps"], resource=resource))

    Package(
        name="decp",
        title="DECP tabulaire",
        description="Données essentielles de la commande publique (FR) au format tabulaire v2.",
        resources=resources,
        # it's possible to provide all the official properties like homepage, version, etc
    ).to_json(DIST_DIR / "datapackage.json")

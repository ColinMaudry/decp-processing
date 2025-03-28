import polars as pl
from httpx import get
import os

from polars.polars import ColumnNotFoundError
from prefect import task
from prefect.futures import wait
from pathlib import Path

from tasks.output import save_to_sqlite



@task(retries=5, retry_delay_seconds=5)
def get_decp_csv(date_now: str, year: str):
    """Téléchargement des DECP publiées par Bercy sur data.economie.gouv.fr."""
    print(f"-- téléchargement du format {year}")
    csv_url = os.getenv(f"DECP_ENRICHIES_VALIDES_{year}_URL")
    if csv_url.startswith("https"):
        # Prod file
        decp_augmente_valides_file: Path = Path(
            f"data/decp_augmente_valides_{year}_{date_now}.csv"
        )
    else:
        # Test file, pas de téléchargement
        decp_augmente_valides_file: Path = Path(csv_url)

    if not (os.path.exists(decp_augmente_valides_file)):
        request = get(
            csv_url,
        )
        with open(decp_augmente_valides_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP d'aujourd'hui déjà téléchargées ({date_now})")

    df: pl.LazyFrame = pl.scan_csv(
        decp_augmente_valides_file,
        low_memory=True,
        separator=";",
        schema_overrides={
            "titulaire_id_1": str,
            "titulaire_id_2": str,
            "titulaire_id_3": str,
            "acheteur.id": str,
            "lieuExecution.code": str,
            # Plus simple de tout mettre en string pour la concaténation des df
            "offresRecues": str,
            "dureeMoisModification": str,
            "montantModification": str,
            "sousTraitanceDeclaree": str,
        },
    )

    if year == "2019":
        df = df.drop(
            "TypePrix"
        )  # SQlite le voit comme un doublon de typePrix, et les données semblent être les mêmes

    df = df.collect()
    save_to_sqlite(df, "datalab", f"data.economie.{year}.ori")
    df = df.lazy()
    df = df.with_columns(
        pl.lit(f"data.economie valides {year}").alias("source_open_data")
    )

    return df


@task
def get_merge_decp(date_now: str):
    df_get = []
    for year in ("2019", "2022"):
        df_get.append(get_decp_csv.submit(date_now, year))

    # On attend que la récupération concurrente des DECP soit terminée
    wait(df_get)

    dfs = {"2019": df_get[0].result(), "2022": df_get[1].result()}

    # Suppression des colonnes abandonnées dans le format 2022
    dfs["2019"] = dfs["2019"].drop(
        [
            "created_at",
            "updated_at",
            "booleanModification",
            # seront réintégrées bientôt depuis les référentiels officiels (SIRENE, etc.)
            "acheteur.nom",
            "lieuExecution.nom",
            "titulaire_denominationSociale_1",
            "titulaire_denominationSociale_2",
            "titulaire_denominationSociale_3",
            # Supprimés pour l'instant, possiblement réintégrées plus tard
            "actesSousTraitance",
            "titulairesModification",
            "modificationsActesSousTraitance",
            "objetModification",
        ]
    )

    # Renommage des colonnes qui ont changé de nom avec le format 2022
    dfs["2019"] = dfs["2019"].rename(
        {
            "technique": "techniques",
            "modaliteExecution": "modalitesExecution",
        }
    )

    # Concaténation des données format 2019 et 2022
    df = pl.concat([dfs["2019"], dfs["2022"]], how="diagonal")
    del dfs

    return df


@task
def get_decp_json(date_now: str):
    import json_stream

    if os.getenv("DECP_JSON_URL").startswith("https"):
        # Prod file
        decp_json_file: Path = Path(f"data/decp_{date_now}.csv")
    else:
        # Test file, pas de téléchargement
        decp_json_file: Path = Path(os.getenv("DECP_JSON_URL"))

    if not (os.path.exists(decp_json_file)):
        request = get(os.getenv("DECP_JSON_URL"))
        with open(decp_json_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP JSON d'aujourd'hui déjà téléchargées ({date_now})")

    return json_stream.load(decp_json_file)


def get_stats():
    url = "https://www.data.gouv.fr/fr/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234"
    df_stats = pl.read_csv(url, index_col=None)
    return df_stats

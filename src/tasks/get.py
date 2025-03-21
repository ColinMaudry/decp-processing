import pandas as pd
from httpx import get
import os
from prefect import task
from prefect.futures import wait
from pathlib import Path

from tasks.output import save_to_sqlite


@task
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
        request = get(csv_url)
        with open(decp_augmente_valides_file, "wb") as file:
            file.write(request.content)
    else:
        print(f"DECP d'aujourd'hui déjà téléchargées ({date_now})")

    df: pd.DataFrame = pd.read_csv(
        decp_augmente_valides_file,
        sep=";",
        dtype=str,
        index_col=None,
    )

    if year == "2019":
        df = df.drop(
            columns=["TypePrix"]
        )  # SQlite le voit comme un doublon de typePrix, et les données semblent être les mêmes

    save_to_sqlite(df, "datalab", f"data.economie.{year}.ori")
    df["source_open_data"] = f"data.economie valides {year}"

    return df


@task
def get_and_merge_decp_csv(date_now: str):
    df_get = []
    formats = []
    for year in ("2019", "2022"):
        df_get.append(get_decp_csv.submit(date_now, year))

    # On attend que la récupération concurrente des DECP soit terminée
    wait(df_get)

    dfs = {"2019": df_get[0].result(), "2022": df_get[1].result()}

    # Suppression des colonnes abandonnées dans le format 2022
    dfs["2019"] = dfs["2019"].drop(
        columns=[
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
    dfs["2019"].rename(
        columns={
            "technique": "techniques",
            "modaliteExecution": "modalitesExecution",
        }
    )

    # Concaténation des données format 2019 et 2022
    df = pd.concat([dfs["2019"], dfs["2022"]], ignore_index=True)

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
    df_stats = pd.read_csv(url, index_col=None)
    return df_stats

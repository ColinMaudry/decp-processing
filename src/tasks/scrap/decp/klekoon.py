import json
import os
import subprocess
from pathlib import Path
from time import sleep

import httpx
import polars as pl
from httpx import Client
from lxml import etree

from src.config import LOG_LEVEL
from src.tasks.publish import publish_scrap_to_datagouv
from src.tasks.utils import get_logger

DCATS_URL = "https://www.klekoon.com/declaration-profil-acheteur"
CLIENT = Client(
    timeout=20, headers={"User-Agent": "decp.info", "Connection": "keep-alive"}
)


def get_dcats(dist_dir) -> list:
    logger = get_logger(level="DEBUG")

    df_dcats = get_dcats_df(dist_dir)
    dcat_paths = []
    dcats_url = df_dcats["urlDCAT"].unique().to_list()
    dcat_dir = dist_dir / "dcats"

    nb_dcats = len(dcats_url)
    logger.debug(f"{nb_dcats} dcats")

    if dcat_dir.exists():
        dcat_paths = os.listdir(dcat_dir)
        dcat_paths = [dcat_dir / path for path in dcat_paths]
        return dcat_paths
    os.makedirs(dcat_dir, exist_ok=True)

    for i, url in enumerate(dcats_url):
        sleep(0.1)
        logger.debug(f"{i}/{nb_dcats} {url} (download)")
        siret = url.split("/")[-1]
        path = Path(f"{dcat_dir}/{siret}.xml")
        with open(path, "w") as f:
            response = CLIENT.get(url).raise_for_status()
            text = response.text
            text: str = text.replace(' encoding="utf-16"', "", 1)
            f.write(text)
        dcat_paths.append(path)

    return dcat_paths


def scrap_klekoon(dist_dir: Path):
    logger = get_logger(level=LOG_LEVEL)

    logger.info("Téléchargement des fichiers DCAT...")
    dcat_paths = get_dcats(dist_dir)

    logger.info("Parsing des DCATs pour récupérer les URLs de marchés...")
    urls = extract_urls_from_dcat(dcat_paths)

    logger.info("Téléchargement des marchés JSON...")
    marches_per_month = get_marches_json(urls, dist_dir)

    logger.info("Concaténation des JSON par mois...")
    json_months_paths = concat_json_per_month(marches_per_month, dist_dir)

    logger.info("Publication...")
    for path in json_months_paths:
        year_month = path.split("/")[-1].split(".")[0].split("_")[1]
        year, month = year_month.split("-")

        publish_scrap_to_datagouv(
            year=year, month=month, file_path=path, target="klekoon"
        )


def extract_urls_from_dcat(dcat_paths: list):
    logger = get_logger(level=LOG_LEVEL)
    len_dcat_paths = len(dcat_paths)

    marches_urls = []

    namespaces = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "dcat": "http://www.w3.org/ns/dcat#",
        "dct": "http://purl.org/dc/terms/",
    }

    for i, dcat_path in enumerate(dcat_paths):
        logger.debug(f"{i}/{len_dcat_paths} {dcat_path} (parsing)")

        urls = []

        try:
            tree: etree._ElementTree = etree.parse(dcat_path)
            datasets = tree.findall(".//dcat:dataset", namespaces=namespaces)

            for dataset in datasets:
                ressources = dataset.findall(
                    ".//{http://www.w3.org/2000/01/rdf-schema#}ressource",
                    namespaces=namespaces,
                )
                month_publication: etree._Element = dataset.find(
                    "dct:issued", namespaces=namespaces
                )
                url = ""

                for ressource in ressources:
                    url: str = ressource.text.strip()
                    if url and "/json/" in url:
                        break

                try:
                    assert url != ""
                except AssertionError:
                    continue

                if month_publication is not None:
                    month_publication: str = month_publication.text.strip()[:7]
                else:
                    try:
                        month_publication: str = get_month_publication(url)
                    except httpx.HTTPStatusError:
                        logger.error(f"URL {url} 404")
                        continue

                urls.append({"url": url, "month": month_publication})

            logger.debug(f"{i}/{len_dcat_paths} {len(urls)} marchés")
            marches_urls.extend(urls)

        except (AssertionError, etree.XMLSyntaxError) as e:
            logger.error(f"{dcat_path}: {e}")

    return marches_urls


def get_marches_json(urls, dist_dir):
    logger = get_logger(level=LOG_LEVEL)

    marches_per_month = {}
    nb_urls = len(urls)
    urls = sorted(urls, key=lambda x: x["month"])

    for i, url in enumerate(urls):
        sleep(0.1)
        logger.debug(f"{i}/{nb_urls} {url}")
        month = url["month"]
        marche_url = url["url"]

        os.makedirs(f"{dist_dir}/{month}", exist_ok=True)

        if month not in marches_per_month:
            marches_per_month[month] = []

        id_marche = marche_url.rsplit("/")[-1]
        path_marche = f"{dist_dir}/{month}/{id_marche}.json"
        with open(path_marche, "w") as f:
            try:
                response = CLIENT.get(marche_url).raise_for_status().json()
                json.dump(response, f)
            except httpx.HTTPStatusError as e:
                logger.error(f"{marche_url}: {e}")
                continue

        marches_per_month[month].append(path_marche)

    return marches_per_month


def concat_json_per_month(marches_per_month, dist_dir):
    json_months_paths = []
    for month in marches_per_month:
        marches_json_month = []

        for marche in marches_per_month[month]:
            with open(marche, "rb") as f:
                marche_json = json.load(f)
            del marche_json["$schema"]
            marche_json["source"] = "scrap_klekoon"
            marches_json_month.append(marche_json)

        marches_json_month = {"marches": marches_json_month}
        path = f"{dist_dir}/klekoon_{month}.json"
        with open(path, "w") as f:
            json.dump(marches_json_month, f, indent=2)
        json_months_paths.append(path)

    return json_months_paths


def get_dcats_df(dist_dir):
    # Exécuter curl sans décoder automatiquement
    path = f"{dist_dir}/dcat.csv"
    if not (os.path.exists(path)):
        result = subprocess.run(
            ["curl", "-s", "-L", DCATS_URL], capture_output=True, check=True
        )
        # Décoder manuellement en ISO-8859-1 (ou Windows-1252)
        content = result.stdout.decode("iso-8859-1")
        content = content.replace(";coordonnnees", ";coordonnnees\n")

        with open(path := f"{dist_dir}/dcat.csv", "w") as f:
            f.write(content)

    # Lire avec Polars
    df = pl.read_csv(path, separator=";", encoding="iso-8859-1")
    return df


def get_month_publication(url) -> str:
    marche_json = CLIENT.get(url).raise_for_status().json()
    date_publication = marche_json["datePublicationDonnees"]
    month_publication = date_publication[:7]
    return month_publication

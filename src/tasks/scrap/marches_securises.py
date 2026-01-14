import json
from pathlib import Path
from time import sleep

import httpx
from bs4 import BeautifulSoup
from prefect import task

from src.config import LOG_LEVEL
from src.tasks.publish import publish_scrap_to_datagouv
from src.tasks.utils import get_logger


def get_html(url: str, client: httpx.Client) -> str or None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    logger = get_logger(level=LOG_LEVEL)

    def get_response() -> httpx.Response:
        return client.get(url, timeout=timeout, headers=headers).raise_for_status()

    timeout = httpx.Timeout(20.0, connect=60.0, pool=20.0, read=20.0)
    try:
        response = get_response()
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
        logger.debug("3s break and retrying...")
        sleep(3)
        try:
            response = get_response()
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
            logger.error("Skipped")
            return None
    html = response.text
    return html


def get_json_marches_securises(url: str, client: httpx.Client) -> dict or None:
    json_html_page = get_html(url, client)
    logger = get_logger(level=LOG_LEVEL)

    sleep(0.1)
    if json_html_page:
        json_html_page = (
            json_html_page.replace("</head>", "</head><body>") + "</body></html>"
        )
    else:
        logger.warning("json_html_page is None, skipping...")
        return None
    json_html_page_soup = BeautifulSoup(json_html_page, "html.parser")
    try:
        decp_json = json.loads(json_html_page_soup.find("body").string)
    except Exception as e:
        logger.info(json_html_page)
        logger.info(e)
        return None
    return decp_json


@task(log_prints=True)
def scrap_marches_securises_month(year: str, month: str, dist_dir: Path):
    logger = get_logger(level=LOG_LEVEL)

    marches = []
    page = 1
    with httpx.Client() as client:
        while True:
            search_url = (
                f"https://www.marches-securises.fr/entreprise/?module=liste_donnees_essentielles&page={str(page)}&siret_pa=&siret_pa1=&date_deb={year}-{month}-01&date_fin={year}-{month}-31&date_deb_ms={year}-{month}-01&date_fin_ms={year}-{month}-31&ref_ume=&cpv_et=&type_procedure=&type_marche=&objet=&rs_oe=&dep_liste=&ctrl_key=aWwwS1pLUlFzejBOYitCWEZzZTEzZz09&text=&donnees_essentielles=1&search="
                f"table_ms&"
            )

            def parse_result_page():
                html_result_page = get_html(search_url, client)
                if html_result_page is None:
                    return []
                soup = BeautifulSoup(html_result_page, "html.parser")
                result_div = soup.find("div", attrs={"id": "liste_consultations"})
                logger.info(f"Year: {year}, Month: {month}, Page: {str(page)}")
                return result_div.find_all(
                    "a", attrs={"title": "Télécharger au format Json"}
                )

            try:
                json_links = parse_result_page()
            except AttributeError:
                sleep(1)
                logger.info("Retrying result page download and parsing...")
                json_links = parse_result_page()

            if not json_links:
                break
            else:
                page += 1
            for json_link in json_links:
                json_href = "https://www.marches-securises.fr" + json_link["href"]
                decp_json = get_json_marches_securises(json_href, client)
                marches.append(decp_json)
        if len(marches) > 0:
            dicts = {"marches": marches}
            json_path = dist_dir / f"marches-securises_{year}-{month}.json"
            with open(json_path, "w") as f:
                f.write(json.dumps(dicts))
            publish_scrap_to_datagouv(year, month, json_path, "marches-securises.fr")

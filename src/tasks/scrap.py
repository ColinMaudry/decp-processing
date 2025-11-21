import calendar
import json
import re
import time
from datetime import date, timedelta
from pathlib import Path
from time import sleep

import httpx
from bs4 import BeautifulSoup
from prefect import task
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait

from src.tasks.publish import publish_scrap_to_datagouv


def get_html(url: str, client: httpx.Client) -> str or None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    def get_response() -> httpx.Response:
        return client.get(url, timeout=timeout, headers=headers).raise_for_status()

    timeout = httpx.Timeout(20.0, connect=60.0, pool=20.0, read=20.0)
    try:
        response = get_response()
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
        print("3s break and retrying...")
        sleep(3)
        try:
            response = get_response()
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
            print("Skipped")
            return None
    html = response.text
    return html


# @task(
#     cache_policy=INPUTS,
#     persist_result=True,
#     cache_expiration=datetime.timedelta(days=15),
# )
def get_json_marches_securises(url: str, client: httpx.Client) -> dict or None:
    json_html_page = get_html(url, client)
    sleep(0.1)
    if json_html_page:
        json_html_page = (
            json_html_page.replace("</head>", "</head><body>") + "</body></html>"
        )
    else:
        print("json_html_page is None, skipping...")
        return None
    json_html_page_soup = BeautifulSoup(json_html_page, "html.parser")
    try:
        decp_json = json.loads(json_html_page_soup.find("body").string)
    except Exception as e:
        print(json_html_page)
        print(e)
        return None
    return decp_json


@task(log_prints=True)
def scrap_marches_securises_month(year: str, month: str, dist_dir: Path):
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
                print("Year: ", year, "Month: ", month, "Page: ", str(page))
                return result_div.find_all(
                    "a", attrs={"title": "Télécharger au format Json"}
                )

            try:
                json_links = parse_result_page()
            except AttributeError:
                sleep(3)
                "Retrying result page download and parsing..."
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


@task(log_prints=True)
def scrap_aws_month(year: str = None, month: str = None, dist_dir: Path = None):
    options = Options()
    options.add_argument("--headless")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", str(dist_dir))

    driver = webdriver.Firefox(options=options)
    driver.implicitly_wait(10)  # secondes

    end_date = start_date = date(int(year), int(month), 1)
    base_duration = timedelta(days=3)
    nb_days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
    last_month_day = start_date + timedelta(days=nb_days_in_month - 1)
    marches_month = []
    replacements = httpx.get(
        "https://www.data.gouv.fr/api/1/datasets/r/3bdd5a64-c28e-4c6a-84fd-5a28bcaa53e9",
        follow_redirects=True,
    ).json()

    while end_date < last_month_day:
        start_date_str = start_date.isoformat()
        end_date = start_date + base_duration

        if end_date > last_month_day:
            end_date = last_month_day

        driver.get("https://www.marches-publics.info/Annonces/rechercher")

        def search_form(end_date_: date) -> tuple[date, int]:
            end_date_str_ = end_date_.isoformat()
            sleep(1)
            print(f"➡️  {start_date_str} -> {end_date_str_}")

            # Formulaire recherche données essentielles
            form = driver.find_element(By.ID, "formRech")
            de_radio = form.find_element(By.ID, "typeDE")
            de_radio.click()

            # Remplir le formulaire
            notif_debut = form.find_element(By.ID, "dateNotifDebut")
            notif_debut.clear()
            notif_debut.send_keys(start_date_str)
            notif_fin = form.find_element(By.ID, "dateNotifFin")
            notif_fin.clear()
            notif_fin.send_keys(end_date_str_)
            sleep(0.1)
            form.find_element(By.ID, "sub").click()
            sleep(1)

            # Soit le bouton de téléchargement apparaît, soit il y a une erreur parce
            # que de trop nombreux résultats sont retournés
            result, nb_results = wait_for_either_element(driver)

            if result == "error":
                # On réessaie avec moins de réultats
                if end_date_ != start_date:
                    end_date_ = search_form(end_date_ - timedelta(days=1))
                else:
                    print("start_date == end_date et trop de résultats !")
            elif result is None:
                print("Pas de téléchargement, on skip.")
            elif result == "timeout":
                # On réessaie
                end_date_ = search_form(end_date_)

            # End search_form()
            return end_date, nb_results

        end_date, nb_results = search_form(end_date)
        end_date_str = end_date.isoformat()

        json_path = dist_dir / "donneesEssentielles.json"

        start_time = time.time()
        last_size = 0
        timeout = 10
        downloaded = False
        final_json_path = dist_dir / f"{start_date_str}_{end_date_str}.json"

        while time.time() - start_time < timeout and downloaded is False:
            if json_path.exists():
                current_size = json_path.stat().st_size
                if current_size == last_size and current_size > 0:
                    print(f"{json_path.name} téléchargé.")
                    sleep(0.1)
                    json_path.rename(final_json_path)
                    downloaded = True
                last_size = current_size
            time.sleep(0.2)

        if final_json_path.exists():
            print("json path exists")
            with open(final_json_path, "r") as f:
                json_text = f.read()
            try:
                marches = json.loads(json_text)["marches"]
            except json.decoder.JSONDecodeError:
                print("Le décodage JSON a échoué, tentative de correction...")

                def fix_unescaped_quotes_in_objet(text):
                    """Générée avec l'aide de ChatGPT (GPT-4o)"""

                    # Match the value of "objet" up to the "montant" key
                    def replacer(match):
                        # Escape quotes that are not already escaped
                        fixed_objet = re.sub(r'(?<!\\)"', r"\"", match.group(1))
                        return f'"objet":"{fixed_objet}","'

                    fixed_text = re.sub(r'"objet":"(.*?)","', replacer, text)
                    return fixed_text

                json_text = fix_unescaped_quotes_in_objet(json_text)

                # Autres remplacements pour obtenir un JSON valide
                for key in replacements.keys():
                    json_text = json_text.replace(key, replacements[key])
                marches = json.loads(json_text)["marches"]
            if len(marches) == nb_results:
                marches_month.extend(marches)
                print(
                    f"✅  Téléchargement valide, longueur marchés {len(marches)} (mois : {len(marches_month)})"
                )

                # On passe aux jours suivants
                start_date = end_date + timedelta(days=1)
            else:
                # On reste sur les mêmes jours
                print(
                    "Résultats de recherche différents marchés téléchargés, on réessaie..."
                )
        else:
            # On reste sur les mêmes jours
            print("Aucun fichier téléchargé. On réessaie...")

    driver.close()

    if len(marches_month) > 0 and isinstance(marches_month, list):
        # Format 2022, donc double niveau
        dicts = {"marches": {"marche": marches_month}}
        json_path = dist_dir / f"aws_{year}-{month}.json"
        with open(json_path, "w") as f:
            f.write(json.dumps(dicts))
        publish_scrap_to_datagouv(year, month, json_path, "aws")

    # End scrap AWS


def wait_for_either_element(driver, timeout=10) -> tuple[str or None, int]:
    """
    Attend de voir si le bouton de téléchargement apparaît ou bien le message d'erreur.
    Fonction générée en grande partie avec la LLM Euria, développée par Infomaniak
    """
    download_button_id = "downloadDonnees"

    try:
        # Wait for either element to appear
        wait = WebDriverWait(driver, timeout)
        result = wait.until(
            lambda d: (
                d.find_element(By.ID, download_button_id)
                if d.find_elements(By.ID, download_button_id)
                else None
            )
            or (
                d.find_element(By.CLASS_NAME, "alert")
                if d.find_elements(By.CLASS_NAME, "alert")
                else None
            )
        )

        # Determine which one appeared
        if result.get_attribute("id") == download_button_id:
            nb_results = (
                driver.find_element(By.ID, "content")
                .find_element(By.CLASS_NAME, "full")
                .find_element(By.TAG_NAME, "h2")
                .find_element(By.TAG_NAME, "strong")
                .text.strip()
            )
            nb_results = int(nb_results)
            # Résulats de recherche OK
            result.click()
            print("download intent...")
            sleep(2)
            return "download", nb_results
        elif "recherche" in result:
            print("too many results")
            return "error", 0
        else:
            print("Ni téléchargement, ni erreur...")
            return None, 0  # Should not happen

    except TimeoutException:
        print("[Timeout] Ni bouton ni erreur dans le temps imparti...")
        return "timeout", 0
    except Exception as e:
        print(f"[Error] Unexpected error while waiting: {e}")
        return None, 0

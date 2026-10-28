import calendar
import json
import time
from datetime import date
from pathlib import Path

import httpx
from prefect import task
from prefect_email import EmailServerCredentials, email_send_message

from src.config import LOG_LEVEL
from src.tasks.publish import publish_scrap_to_datagouv
from src.tasks.utils import get_logger

# Identifiants publics embarqués dans le JavaScript du site marches-securises.fr,
# utilisés pour le flux client_credentials de l'API wsp.atline.fr. Ce ne sont pas des secrets.
ATLINE_CLIENT_ID = "atline-services"
ATLINE_CLIENT_SECRET = "22360C1B138EA4EA935F1B28FB1B16CB"

ATLINE_BASE_URL = "https://wsp.atline.fr"
ATLINE_LIST_ENDPOINT = f"{ATLINE_BASE_URL}/donnees_essentielles_oe_list"
ATLINE_TOKEN_ENDPOINT = f"{ATLINE_BASE_URL}/token"

# Le site n'affiche que 10 marchés par page, mais l'API accepte jusqu'à 50.
ATLINE_PAGE_SIZE = 50

ATLINE_TIMEOUT = httpx.Timeout(20.0, connect=60.0, pool=20.0, read=20.0)


def get_atline_token(client: httpx.Client) -> str:
    """Token d'accès à l'API Atline (validité 12 h)."""
    response = client.post(
        ATLINE_TOKEN_ENDPOINT,
        json={
            "client_id": ATLINE_CLIENT_ID,
            "client_secret": ATLINE_CLIENT_SECRET,
            "grant_type": "client_credentials",
            "redirect_uri": "",
        },
        headers={"Origin": "https://www.marches-securises.fr"},
        timeout=ATLINE_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_marches_page(
    client: httpx.Client,
    token: str,
    start_date: str,
    end_date: str,
    page: int,
    retries: int = 2,
) -> dict | None:
    """Une page de la liste des données essentielles pour une plage de dates
    de publication des données.

    Retourne None si les requêtes échouent après les retries."""
    logger = get_logger(level=LOG_LEVEL)

    for attempt in range(retries + 1):
        try:
            response = client.post(
                ATLINE_LIST_ENDPOINT,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Origin": "https://www.marches-securises.fr",
                    "Referer": "https://www.marches-securises.fr/",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                },
                json={
                    "plateform": "MS",
                    "identifier": "",
                    "keyword": "",
                    "search": "",
                    "start_date": start_date,
                    "end_date": end_date,
                    "departments": [],
                    "target": "",
                    "pagination": {
                        "page": page,
                        "nbr_par_page": ATLINE_PAGE_SIZE,
                        # date_creation : ordre déterministe (id_dma n'est pas triable)
                        "ordre": "date_creation",
                        "sens_ordre": "asc",
                    },
                },
                timeout=ATLINE_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError) as e:
            logger.warning(
                f"Page {page} ({start_date} -> {end_date}) : {type(e).__name__}, "
                f"tentative {attempt + 1}/{retries + 1}"
            )
            if attempt < retries:
                time.sleep(3)
    logger.error(f"Page {page} ({start_date} -> {end_date}) : abandon")
    return None


def atline_to_decp(record: dict) -> dict:
    """Convertit un record de l'API donnees_essentielles_oe_list au format
    DECP (schéma marches.json, format 2019).

    Champs de l'API absents du schéma cible : is_demo, niump, id_dma, version,
    etat_marche, user_nom, user_prenom, date_aife, ref_fonct_aife,
    date_update_aife, date_creation, is_concession (transformé en _type).
    Champs du schéma cible absents de l'API : techniques, modalitesExecution,
    offresRecues, attributionAvance, tauxAvance, ccag, sousTraitanceDeclaree,
    typeGroupementOperateurs, origineUE, origineFrance, considerationsSociales,
    considerationsEnvironnementales, typesPrix, marcheInnovant, idAccordCadre,
    modifications. Ils seront null dans le parquet consolidé, pas bloquant.
    """
    # Format DECP 2019 : structs plats (sans la clé "titulaire" du format 2022)
    titulaires = [
        {
            "typeIdentifiant": t.get("type_ident") or "SIRET",
            "id": t.get("ident"),
        }
        for t in (record.get("titulaires") or [])
        if t and t.get("ident")
    ]
    marche = {
        "id": record.get("num_lot"),
        "nature": record.get("nature_marche"),
        "objet": record.get("objet_marche"),
        "codeCPV": record.get("code_cpv"),
        "procedure": record.get("procedure"),
        "dureeMois": record.get("duree"),
        "dateNotification": record.get("date_notif"),
        "datePublicationDonnees": record.get("date_publi_donnee"),
        "montant": record.get("mtt_estime"),
        "formePrix": record.get("forme_prix"),
        "acheteur": {"id": record.get("siret_pa")},
        "titulaires": titulaires,
        "lieuExecution": {
            "code": record.get("code_lieux"),
            "typeCode": record.get("type_code_lieux"),
        },
        "_type": "Contrat de concession" if record.get("is_concession") else "Marché",
    }
    # Les champs absents des données sources ne sont pas écrits (pas de null
    # explicite) : l'ingestion ajoute les champs manquants en null de toute façon.
    return {k: v for k, v in marche.items() if v is not None}


def build_uid(marche: dict) -> str:
    """uid d'un marché : acheteur_id + id + code CPV (convention decp-processing).
    Distingue les marchés qui partagent le même (acheteur_id, id) avec des CPV
    différents."""
    return (
        f"{marche.get('acheteur', {}).get('id', '')}{marche.get('id', '')}"
        f"_{marche.get('codeCPV', '')}"
    )


def notify_no_data(year: str, month: str, start_date: str, end_date: str):
    """Envoie un email d'alerte si le scrap n'a retourné aucun marché.

    L'échec de l'envoi est loggé sans lever d'exception : l'alerte ne doit pas
    masquer le résultat du scrap."""
    logger = get_logger(level=LOG_LEVEL)
    try:
        email_server_credentials = EmailServerCredentials.load("email-notifier")
        email_send_message(
            email_server_credentials=email_server_credentials,
            subject=f"[decp-processing] Scrap marches-securises.fr vide : {month}/{year}",
            msg=(
                f"Le scrap de marches-securises.fr n'a retourné aucun marché pour "
                f"la fenêtre {start_date} -> {end_date}.\n\n"
                f"Vérifier l'API https://wsp.atline.fr/donnees_essentielles_oe_list "
                f"(token, paramètres, disponibilité du service)."
            ),
            email_to=email_server_credentials.username,
        )
    except Exception as e:
        logger.error(f"Échec d'envoi de l'alerte email scrap vide : {e}")


@task(log_prints=True)
def scrap_marches_securises_month(year: str, month: str, dist_dir: Path):
    logger = get_logger(level=LOG_LEVEL)

    start_date = date(int(year), int(month), 1)
    last_day = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = date(start_date.year, start_date.month, last_day)

    marches = []
    page = 1
    with httpx.Client() as client:
        token = get_atline_token(client)
        while True:
            logger.info(f"Year: {year}, Month: {month}, Page: {page}")
            data = get_marches_page(
                client, token, start_date.isoformat(), end_date.isoformat(), page
            )
            # Une page ratée après retries n'interrompt pas le scrap des autres
            # pages, mais on ne peut pas savoir s'il restait des marchés :
            # on arrête la pagination.
            if data is None:
                break
            donnees = data.get("donnees", [])
            marches.extend(atline_to_decp(record) for record in donnees)
            nb_total = data.get("nb_total", 0)
            if not donnees or len(marches) >= nb_total:
                break
            page += 1

    if len(marches) > 0:
        for marche in marches:
            # L'uid est déduit des autres champs, l'API ne le fournit pas
            marche["uid"] = build_uid(marche)
        dicts = {"marches": marches}
        json_path = dist_dir / f"marches-securises_{year}-{month}.json"
        dist_dir.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w") as f:
            f.write(json.dumps(dicts, ensure_ascii=False))
        logger.info(f"{len(marches)} marchés pour le mois {month}/{year}")
        publish_scrap_to_datagouv(year, month, json_path, "marches-securises.fr")
    else:
        logger.warning(
            f"Aucun marché scrapé pour {month}/{year} "
            f"({start_date.isoformat()} -> {end_date.isoformat()})"
        )
        notify_no_data(year, month, start_date.isoformat(), end_date.isoformat())

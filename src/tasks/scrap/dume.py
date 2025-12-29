import calendar
import json
from datetime import date, timedelta
from pathlib import Path
from time import sleep

import dume_api
import polars as pl

from config import DIST_DIR, LOG_LEVEL
from tasks.publish import publish_scrap_to_datagouv
from tasks.utils import get_logger


def scrap_dume_month(year: str = None, month: str = None, dist_dir: Path = None):
    logger = get_logger(level=LOG_LEVEL)

    end_date = start_date = date(int(year), int(month), 1)
    base_duration = timedelta(days=0)
    nb_days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
    last_month_day = start_date + timedelta(days=nb_days_in_month - 1)
    marches_month = []
    decp_uids = set(
        pl.read_parquet(DIST_DIR / "decp.parquet", columns=["uid"])["uid"].to_list()
    )

    retry_count = 0

    while end_date < last_month_day:
        # On évite de boucler sans fin
        if retry_count > 3:
            logger.error("Trop d'essais, start_date + 1")
            retry_count = 0
            start_date = end_date + timedelta(days=1)
            continue

        start_date_str = start_date.isoformat()

        if retry_count == 0:
            end_date = start_date + base_duration

        if end_date > last_month_day:
            end_date = last_month_day

        end_date_str = end_date.isoformat()

        logger.info(f"➡️  {start_date_str} -> {end_date_str}")
        rows, nb_results = get_dume_rows(start_date_str, end_date_str)

        marches = []
        if nb_results > 0:
            marches = dume_to_decp(rows)

        marches_month.extend(marches)

        # On passe aux jours suivants
        start_date = end_date + timedelta(days=1)
        retry_count = 0
        continue

    if len(marches_month) > 0:
        # Format 2022, donc double niveau
        dicts = {"marches": {"marche": marches_month}}
        json_path = dist_dir / f"dume_{year}-{month}.json"
        with open(json_path, "w") as f:
            f.write(json.dumps(dicts, indent=2))
        logger.info(str(len(marches_month)) + f" marchés pour le mois ({month}/{year})")

        get_uid_stats(marches_month, decp_uids)
        publish_scrap_to_datagouv(year, month, json_path, "dume")


def dume_to_decp(rows):
    new_rows = []

    def get_titulaires(titulaires):
        if isinstance(titulaires, list) and len(titulaires) > 0:
            return [{"titulaire": titulaire} for titulaire in d.get("titulaires")]
        return []

    for r in rows:
        d = r.get("donneesMP")
        new_row = {
            "uid": d.get("idAcheteur") + r.get("id"),
            "id": r.get("id"),
            "objet": r.get("objet"),
            "nature": r.get("nature"),
            "procedure": r.get("procedure"),
            "dureeMois": r.get("dureeMois"),
            "datePublicationDonnees": r.get("datePublicationDonnees"),
            "acheteur": {
                "id": d.get("idAcheteur"),
            },
            "techniques": [
                {"technique": [d.get("technique")]},
            ],
            "modalitesExecution": [
                {"modaliteExecution": [d.get("modaliteExecution")]},
            ],
            "idAccordCadre": d.get("idAccordCadre"),
            "codeCPV": d.get("codeCPV"),
            "lieuExecution": {
                "code": d.get("lieuExecutionCode"),
                "typeCode": d.get("lieuExecutionTypeCode"),
            },
            "dateNotification": d.get("dateNotification"),
            "marchesInnovant": d.get("marchesInnovant"),
            "attributionAvance": d.get("attributionAvance"),
            "tauxAvance": d.get("tauxAvance"),
            "origineUE": d.get("origineUE"),
            "origineFrance": d.get("origineFrance"),
            "ccag": d.get("ccag"),
            "offresRecues": d.get("offresRecues"),
            "montant": d.get("montant"),
            "formePrix": d.get("formePrix"),
            "typesPrix": {"typePrix": [d.get("typePrix")]},
            "typeGroupementOperateurs": d.get("typeGroupementOperateurs"),
            "sousTraitanceDeclaree": d.get("sousTraitanceDeclaree"),
            "titulaires": get_titulaires(d.get("titulaires")),
            "modifications": r.get("modifications"),
            "source": "scrap_aife_dume",
        }
        new_rows.append(new_row)

    return new_rows


def get_dume_rows(start_date_str: str, end_date_str: str) -> tuple[list[dict], int]:
    procedures = [
        "Marché passé sans publicité ni mise en concurrence préalable",
        "Appel d'offres ouvert",
        "Appel d'offres restreint",
        "Procédure adaptée",
        "Procédure avec négociation",
        "Dialogue compétitif",
    ]

    logger = get_logger(level=LOG_LEVEL)

    _rows = []

    for procedure in procedures:
        try:
            new_rows = dume_api.get_contracts(
                date_start=start_date_str,
                date_end=end_date_str,
                type_de="MP",
                procedure=procedure,
                partition_map={
                    "nature": [
                        "Marché",
                        "Marché de partenariat",
                        "Accord-cadre",
                        "Marché subséquent",
                    ]
                },
            )
            sleep(0.1)
            _rows.extend(new_rows)
            logger.debug(procedure + " " + str(len(new_rows)) + f" rows ({len(_rows)})")
        except RuntimeError:
            logger.error(f"Trop de lignes, on passe ({procedure})")

    _nb_results = len(_rows)
    logger.debug(str(_nb_results) + " rows pour la période")

    return _rows, _nb_results


def get_uid_stats(marches, decp_uids):
    logger = get_logger(level=LOG_LEVEL)

    dume_uids = {marche["uid"] for marche in marches}

    in_decp_uids = dume_uids.intersection(decp_uids)

    logger.info(
        str(len(dume_uids)) + " identifiants uniques dans le DUME pour cette période"
    )
    logger.info(
        str(len(in_decp_uids))
        + " identifiants uniques dans le DUME pour cette période présents dans les DECP consolidées"
    )
    logger.info(
        str(round(len(in_decp_uids) / len(dume_uids) * 100, 2))
        + " % des identifiants sur cette période sont présents dans les DECP consolidées tabulaires"
    )

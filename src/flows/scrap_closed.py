"""Flow scrap_closed : scrapers de plateformes fermées, puis traitement (issue #178)."""

import contextvars
from datetime import date

import polars as pl
from prefect import flow, tags

from src import config
from src.config import LOG_LEVEL, MAX_PREFECT_WORKERS
from src.flows.deao_processing import deao_processing
from src.tasks.deao.schema import published_schema
from src.tasks.deao.sources_publiees import ecrire_et_publier, publication_autorisee
from src.tasks.scrap.deao.contrat import SCRAPERS
from src.tasks.scrap.deao.erreurs import ScrapPartiel
from src.tasks.utils import get_logger, run_in_pool

# Les références de module (SCRAPERS, deao_processing) et config.* sont lues à
# l'appel : les tests les substituent.


def _codes_a_executer() -> list[str]:
    solo = [s.strip() for s in config.SOLO_DEAO_SCRAPS if s.strip()]
    if solo:
        return solo
    return [e.code for e in config.DEAO_REGISTRE if not e.open_data and not e.skip]


@flow(log_prints=True)
def scrap_closed(run_date: str | None = None) -> None:
    logger = get_logger(level=LOG_LEVEL)
    logger.info("🚀  Début du flow scrap_closed")
    today = date.fromisoformat(run_date) if run_date else date.today()
    entrees = {e.code: e for e in config.DEAO_REGISTRE}
    codes = _codes_a_executer()
    erreurs: dict[str, str] = {}
    schema = published_schema()

    for code in codes:
        if code not in SCRAPERS:
            erreurs[code] = "aucun scraper enregistré"
        elif code not in entrees:
            erreurs[code] = "source absente du registre"
    a_lancer = [c for c in codes if c not in erreurs]

    # Un contexte par scraper, copié dans le fil du flow : les fils du pool gardent
    # le contexte Prefect (logger du run, sous-flow sirene_preprocess).
    contextes = {code: contextvars.copy_context() for code in a_lancer}
    # Scrapers en échec partiel : lignes publiées, puis erreur du flow.
    partiels: dict[str, str] = {}

    def executer(code: str) -> pl.DataFrame:
        try:
            return contextes[code].run(SCRAPERS[code], today)
        except ScrapPartiel as e:
            logger.error(f"❌ Échec partiel du scraper {code} : {e}")
            partiels[code] = str(e)
            return e.lignes

    resultats = run_in_pool(
        executer,
        a_lancer,
        max_workers=MAX_PREFECT_WORKERS,
        logger=logger,
        label=lambda c: f"scraper {c}",
    )
    reussis = {code: df for code, df in resultats}
    for code in a_lancer:
        if code not in reussis:
            erreurs[code] = "le scraper a échoué"

    if not publication_autorisee():
        logger.info(
            "Publication des fichiers par source désactivée "
            "(DECP_PROCESSING_PUBLISH ou SOLO_DEAO_SCRAPS)."
        )
    config.DEAO_DIST_DIR.mkdir(parents=True, exist_ok=True)
    for code, df in reussis.items():
        if set(df.columns) != set(schema):
            ecart = sorted(set(df.columns) ^ set(schema))
            erreurs[code] = f"colonnes non conformes au schéma publié ({ecart})"
            continue
        etrangeres = df.filter(
            pl.col("sourceDataset").is_null() | (pl.col("sourceDataset") != code)
        ).height
        if etrangeres:
            erreurs[code] = (
                f"{etrangeres} ligne(s) dont sourceDataset != {code}, non publié"
            )
            continue
        lignes = df.select(pl.col(c).cast(t) for c, t in schema.items())
        cause = ecrire_et_publier(
            code, lignes, entrees[code], config.DEAO_DIST_DIR / f"{code}.parquet"
        )
        if cause:
            erreurs[code] = cause
    for code, cause in partiels.items():
        erreurs[code] = f"{erreurs[code]} ; {cause}" if code in erreurs else cause

    # Le traitement agrège ce qui est publié sur data.gouv.fr, même en cas d'échec partiel.
    with tags("appels-offres", "traitement"):
        deao_processing(run_date=run_date)

    if erreurs:
        detail = " ; ".join(f"{c} : {m}" for c, m in erreurs.items())
        raise RuntimeError(f"scrap_closed en échec pour : {detail}.")
    logger.info("☑️  Fin du flow scrap_closed.")

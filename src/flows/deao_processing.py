"""Flow deao_processing : appels d'offres consolidés, enrichis et publiés (issue #178)."""

from collections import Counter
from datetime import date

import httpx
import polars as pl
from prefect import flow
from tenacity import RetryError

from src.config import (
    BOAMP_START_DATE,
    DATA_DIR,
    DEAO_DIST_DIR,
    DEAO_PARTITIONS_DIR,
    DEAO_REBUILD,
    DEAO_STATUT_DELAI_JOURS,
    LOG_LEVEL,
    SIRENE_DATA_DIR,
)
from src.flows.sirene_preprocess import sirene_preprocess
from src.tasks.deao.acquire import acquire
from src.tasks.deao.consolidate import consolidate
from src.tasks.deao.enrich import enrich_acheteurs
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA, geocoder_adresses
from src.tasks.deao.report import lignes_rapport, publier_rapport
from src.tasks.deao.schema import (
    AVIS_LOT_SCHEMA,
    PUBLISHED_FIELDS,
    empty_avis_frame,
    write_table_schema,
)
from src.tasks.deao.sources.boamp.parse import source_file_expr
from src.tasks.deao.sources.boamp.source import BOAMP
from src.tasks.output import sink_to_files
from src.tasks.publish import publish_deao_to_datagouv, should_publish_deao
from src.tasks.utils import get_logger, print_all_config

# Les références de module (BOAMP_START_DATE, DEAO_*, SIRENE_DATA_DIR,
# sirene_preprocess, charger_referentiels, geocoder_adresses, publier_rapport) sont lues à l'appel : les tests les substituent.


def charger_referentiels() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    """Unités légales et établissements SIRENE (préparés par sirene_preprocess), COG."""
    chemins = [
        SIRENE_DATA_DIR / "unites_legales.parquet",
        SIRENE_DATA_DIR / "etablissements.parquet",
        DATA_DIR / "code_officiel_geographique.parquet",
    ]
    manquants = [str(p) for p in chemins if not p.exists()]
    if manquants:
        raise FileNotFoundError(
            "Référentiels absents (lancer sirene_preprocess / get_cog) : "
            + ", ".join(manquants)
        )
    ul, etab, cog = chemins
    return pl.scan_parquet(ul), pl.scan_parquet(etab), pl.read_parquet(cog)


def lire_partitions() -> pl.DataFrame:
    fichiers = sorted(DEAO_PARTITIONS_DIR.rglob("*.parquet"))
    if not fichiers:
        return empty_avis_frame()
    # Schéma imposé : une partition écrite avant un changement de contrat est lue
    # avec null pour une colonne ajoutée depuis, et sans une colonne retirée.
    return pl.scan_parquet(
        fichiers,
        schema=AVIS_LOT_SCHEMA,
        missing_columns="insert",
        extra_columns="ignore",
    ).collect()


def selection_publiee(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col(f.name).cast(f.dtype)
        if f.name in df.columns
        else pl.lit(None, dtype=f.dtype).alias(f.name)
        for f in PUBLISHED_FIELDS
    )


def _geocoder_tolerant(today: date, stats: Counter):
    """Géocodeur qui, en cas de panne de la Géoplateforme, n'arrête pas le run :
    aucune adresse géocodée (les replis département s'appliquent)."""

    def geocoder(adresses: pl.DataFrame) -> pl.DataFrame:
        try:
            return geocoder_adresses(adresses, today)
        except (httpx.HTTPError, RetryError) as e:
            stats["geocodage_en_erreur"] += 1
            get_logger(level=LOG_LEVEL).warning(
                f"Géocodage indisponible, poursuite sans géocodage : {e!r}"
            )
            return pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA)

    return geocoder


@flow(log_prints=True)
def deao_processing(run_date: str | None = None) -> None:
    logger = get_logger(level=LOG_LEVEL)
    logger.info("🚀  Début du flow deao_processing")
    print_all_config()

    today = date.fromisoformat(run_date) if run_date else date.today()
    stats: Counter = Counter()

    # SIRENE du mois absent : prétraitement en sous-flow, comme decp_processing.
    if not SIRENE_DATA_DIR.exists():
        logger.info(f"{SIRENE_DATA_DIR} absent, lancement de sirene_preprocess...")
        sirene_preprocess()

    # En tête de flow : un référentiel absent arrête le run avant l'acquisition.
    lf_unites_legales, lf_etablissements, df_cog = charger_referentiels()

    logger.info(f"Acquisition BOAMP {BOAMP_START_DATE} → {today}...")
    stats.update(
        acquire(BOAMP, date.fromisoformat(BOAMP_START_DATE), today, DEAO_REBUILD)
    )

    logger.info("Lecture des partitions et consolidation...")
    consolidation = consolidate(lire_partitions(), today, DEAO_STATUT_DELAI_JOURS)
    stats.update(consolidation.stats)

    logger.info("Enrichissement des acheteurs...")
    df = enrich_acheteurs(
        consolidation.df,
        lf_unites_legales,
        lf_etablissements,
        df_cog,
        geocoder=_geocoder_tolerant(today, stats),
    )

    publier_rapport(lignes_rapport(stats, df))

    logger.info(f"Écriture de {df.height} lignes dans {DEAO_DIST_DIR}...")
    DEAO_DIST_DIR.mkdir(parents=True, exist_ok=True)
    df = df.with_columns(source_file_expr(today.year))
    sink_to_files(selection_publiee(df).lazy(), DEAO_DIST_DIR / "deao")
    write_table_schema(DEAO_DIST_DIR / "schema.json")

    if should_publish_deao(df.height):
        logger.info("Publication DEAO sur data.gouv.fr...")
        publish_deao_to_datagouv(DEAO_DIST_DIR)
    else:
        logger.info(
            "Publication DEAO désactivée (DECP_PROCESSING_PUBLISH, DEAO_DATASET_ID "
            f"ou {df.height} lignes < DEAO_MIN_ROWS_TO_PUBLISH)."
        )

    # R20 : les jours sains sont écrits et publiés, mais le run échoue visiblement.
    if stats["jour_en_erreur"] > 0:
        raise RuntimeError(
            f"{stats['jour_en_erreur']} jour(s) BOAMP en erreur (listing, "
            "téléchargement ou parsing) : ils seront retentés au prochain run."
        )

    logger.info("☑️  Fin du flow deao_processing.")

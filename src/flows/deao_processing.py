"""Flow deao_processing : agrégation des fichiers par source (dataset b privé) et
publication des datasets a (Open Data) et b (complet), issue #178."""

from collections import Counter

import polars as pl
from prefect import flow
from prefect.concurrency.sync import concurrency

from src import config
from src.tasks.deao.report import lignes_rapport_sources, publier_rapport
from src.tasks.deao.schema import open_schema, published_schema, write_table_schema
from src.tasks.deao.sources_publiees import (
    publication_autorisee,
    publier_fichier,
    telecharger_fichier_source,
)
from src.tasks.output import sink_to_files
from src.tasks.publish import publish_deao_to_datagouv, should_publish_deao
from src.tasks.utils import get_logger

# La configuration (config.*) est lue à l'appel : les tests la substituent.

# Colonnes absentes des fichiers publiés avant leur ajout au schéma : complétées
# par des nuls à la lecture (transition de schéma).
COLONNES_TRANSITOIRES = ("acheteur_codePostal", "urlDce", "urlReglementConsultation")


def dedoublonner_sources(df: pl.DataFrame) -> tuple[pl.DataFrame, Counter]:
    """Dédoublonnage entre sources : identité en v1 (la règle viendra avec les
    sources qui se recoupent)."""
    return df, Counter(dedoublonnage_retire=0)


def _verifier_identifiants() -> None:
    """Identifiants nécessaires à la publication : échec avant toute publication."""
    for nom in (
        "DEAO_OPEN_DATASET_ID",
        "DEAO_CLOSED_DATASET_ID",
        "DEAO_CLOSED_PARQUET_RESOURCE_ID",
    ):
        if not getattr(config, nom):
            raise RuntimeError(
                f"{nom} est vide : publication impossible (variable d'environnement à définir)."
            )


def _verifier_fichier(code: str, chemin, min_rows: int) -> pl.DataFrame:
    attendu = published_schema()
    df = pl.read_parquet(chemin)
    # Transition : ancien nom de l'identifiant et colonnes ajoutées depuis
    if "idAvisInitial" in df.columns and "id" not in df.columns:
        df = df.rename({"idAvisInitial": "id"})
    df = df.with_columns(
        pl.lit(None, dtype=attendu[c]).alias(c)
        for c in COLONNES_TRANSITOIRES
        if c not in df.columns
    )
    schema = dict(df.schema)
    if schema != attendu:
        manquantes = sorted(set(attendu) - set(schema))
        en_trop = sorted(set(schema) - set(attendu))
        types = sorted(c for c in set(schema) & set(attendu) if schema[c] != attendu[c])
        raise RuntimeError(
            f"Source {code} : schéma non conforme (colonnes manquantes {manquantes}, "
            f"en trop {en_trop}, types différents {types})"
        )
    df = df.select(list(attendu))
    if df.height < min_rows:
        raise RuntimeError(
            f"Source {code} : {df.height} lignes < minimum attendu {min_rows}"
        )
    autres = df.filter(
        pl.col("sourceDataset").is_null() | (pl.col("sourceDataset") != code)
    )
    if autres.height:
        valeurs = sorted(set(autres["sourceDataset"].fill_null("<null>")))
        raise RuntimeError(
            f"Source {code} : {autres.height} ligne(s) avec un sourceDataset "
            f"différent du code de la source ({valeurs})"
        )
    return df


@flow(log_prints=True)
def deao_processing(run_date: str | None = None) -> None:
    logger = get_logger(level=config.LOG_LEVEL)
    logger.info("🚀  Début du flow deao_processing")

    with concurrency("deao-processing", occupy=1, strict=False):
        sources = [s for s in config.DEAO_REGISTRE if not s.skip]
        if not sources:
            raise ValueError("Aucune source DEAO active dans le registre.")

        if publication_autorisee():
            _verifier_identifiants()

        # Étapes 1 à 3 : toute erreur survient avant la moindre publication.
        dossier = config.DEAO_DATA_DIR / "sources"
        dossier.mkdir(parents=True, exist_ok=True)
        frames = {}
        ecartees = []
        for source in sources:
            logger.info(f"Téléchargement de la source {source.code}...")
            try:
                chemin = telecharger_fichier_source(source, dossier)
                frames[source.code] = _verifier_fichier(
                    source.code, chemin, source.min_rows
                )
            except Exception as e:
                if source.open_data:
                    raise
                # Source fermée en échec : écartée, ne bloque pas les autres
                logger.error(f"Source fermée {source.code} écartée : {e}")
                ecartees.append(source.code)
        if not frames:
            raise RuntimeError("Aucune source DEAO exploitable.")

        df, stats = dedoublonner_sources(pl.concat(list(frames.values())))
        logger.info(f"Dédoublonnage : {dict(stats)}")
        if df["uid"].is_duplicated().any():
            n = int(df["uid"].is_duplicated().sum())
            raise RuntimeError(f"uid non unique dans l'agrégat ({n} lignes concernées)")

        publier_rapport(
            lignes_rapport_sources(df)
            + [{"indicateur": f"source_ecartee:{c}", "valeur": 1} for c in ecartees]
        )

        # Écriture : agrégat Open Data (a) et agrégat complet (b)
        dist = config.DEAO_DIST_DIR
        dist.mkdir(parents=True, exist_ok=True)
        # Concaténation des seules sources open_data (jamais un filtre a posteriori)
        frames_open = [
            frames[s.code] for s in sources if s.open_data and s.code in frames
        ]
        df_open = (
            pl.concat(frames_open).select(list(open_schema()))
            if frames_open
            else pl.DataFrame(schema=open_schema())
        )
        logger.info(f"Écriture de {df_open.height} lignes Open Data dans {dist}...")
        sink_to_files(df_open.lazy(), dist / "deao")
        write_table_schema(dist / "schema.json")
        (dist / "closed").mkdir(exist_ok=True)
        df.write_parquet(dist / "closed" / "deao.parquet", compression="zstd")

        if not publication_autorisee():
            logger.info(
                "Publication DEAO désactivée (DECP_PROCESSING_PUBLISH ou SOLO_DEAO_SCRAPS)."
            )
            return
        if should_publish_deao(df_open.height):
            logger.info("Publication du dataset a (Open Data)...")
            publish_deao_to_datagouv(dist)
        else:
            logger.info(f"Dataset a non publié ({df_open.height} lignes).")
        if df.height >= config.DEAO_MIN_ROWS_TO_PUBLISH:
            logger.info("Publication du dataset b (complet)...")
            publier_fichier(
                config.DEAO_CLOSED_DATASET_ID,
                config.DEAO_CLOSED_PARQUET_RESOURCE_ID,
                dist / "closed" / "deao.parquet",
            )
        else:
            logger.info(f"Dataset b non publié ({df.height} lignes).")

    logger.info("☑️  Fin du flow deao_processing.")

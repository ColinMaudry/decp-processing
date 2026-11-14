"""Briques communes aux scrapers de plateformes DEAO (Atexo, AWS) : fichier publié
précédent, référentiels d'enrichissement, dictionnaire SIRET partagé et fusion des
consultations d'un passage à l'autre."""

import threading
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from src import config
from src.config import DEAO_STATUT_DELAI_JOURS, SourceDeao
from src.flows.sirene_preprocess import sirene_preprocess
from src.tasks.deao import referentiels
from src.tasks.deao.consolidate import statut_temporel_expr
from src.tasks.deao.enrich import dictionnaire_depuis_publie
from src.tasks.deao.geocode_cache import geocoder_adresses  # noqa: F401
from src.tasks.deao.schema import published_schema, selection_publiee
from src.tasks.deao.sources_publiees import (
    FichierSourceNonConforme,
    telecharger_fichier_source,
)
from src.tasks.utils import get_logger

# Les références de module (charger_referentiels, geocoder_adresses,
# sirene_preprocess) et config.* sont lues à l'appel : les tests les substituent.

FUSEAU = ZoneInfo("Europe/Paris")
_COLONNES_DICTIONNAIRE = ("acheteur_nom", "acheteur_id", "acheteur_codePostal")
# Part des consultations ouvertes connues annulées en un passage au-delà de laquelle
# on avertit (une page de liste tronquée sans erreur ressemblerait à cela).
SEUIL_ANNULATIONS = 0.1

# Les plateformes tournent en parallèle : un seul prétraitement SIRENE.
_VERROU_SIRENE = threading.Lock()

# Dictionnaire SIRET partagé par toutes les plateformes d'un passage : boamp.parquet
# n'est téléchargé qu'une fois (succès comme échec) par jour et dossier de données.
_VERROU_DICTIONNAIRE = threading.Lock()
_DICTIONNAIRES: dict[tuple, pl.DataFrame | None] = {}


def charger_referentiels() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    return referentiels.charger_referentiels(config.SIRENE_DATA_DIR, config.DATA_DIR)


def referentiels_enrichissement() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    """Référentiels d'enrichissement ; SIRENE du mois absent : prétraitement, comme
    scrap_open."""
    with _VERROU_SIRENE:
        if not config.SIRENE_DATA_DIR.exists():
            get_logger(level=config.LOG_LEVEL).info(
                f"{config.SIRENE_DATA_DIR} absent, lancement de sirene_preprocess..."
            )
            sirene_preprocess()
    return charger_referentiels()


def lire_precedent(entree: SourceDeao, dossier: Path) -> pl.DataFrame:
    """Fichier publié de la source. Non conforme (ex. fichier provisoire déposé à la
    création de la ressource) : départ de zéro seulement sur demande explicite. Absent,
    en erreur ou illisible : toujours une erreur, même avec le drapeau (une panne de
    data.gouv.fr ne doit pas écraser l'historique)."""
    chemin = telecharger_fichier_source(entree, dossier)
    try:
        try:
            precedent = selection_publiee(pl.read_parquet(chemin))
        except pl.exceptions.PolarsError as e:
            raise FichierSourceNonConforme(
                f"Source {entree.code} : fichier publié non conforme au schéma ({e})"
            ) from e
        etrangeres = precedent.filter(
            pl.col("sourceDataset").is_null() | (pl.col("sourceDataset") != entree.code)
        ).height
        if etrangeres:
            raise FichierSourceNonConforme(
                f"Source {entree.code} : fichier publié non conforme "
                f"({etrangeres} ligne(s) dont sourceDataset != {entree.code})"
            )
        return precedent
    except FichierSourceNonConforme as e:
        if not config.DEAO_SCRAP_PARTIR_DE_ZERO:
            raise
        get_logger(level=config.LOG_LEVEL).warning(
            f"{entree.code} : fichier publié non conforme ({e}), départ de zéro "
            "(DEAO_SCRAP_PARTIR_DE_ZERO)."
        )
        return pl.DataFrame(schema=published_schema())


def _dictionnaire(code: str, dossier: Path) -> pl.DataFrame | None:
    """Dictionnaire SIRET appris sur `boamp.parquet` publié ; None en cas d'échec."""
    logger = get_logger(level=config.LOG_LEVEL)
    boamp = next((e for e in config.DEAO_REGISTRE if e.code == "boamp"), None)
    try:
        if boamp is None:
            raise ValueError("source boamp absente du registre")
        chemin = telecharger_fichier_source(boamp, dossier)
        presentes = pl.read_parquet_schema(chemin)
        colonnes = [c for c in _COLONNES_DICTIONNAIRE if c in presentes]
        dictionnaire = dictionnaire_depuis_publie(
            pl.read_parquet(chemin, columns=colonnes)
        )
    except Exception as e:
        logger.warning(
            f"{code} : pas de dictionnaire SIRET pour ce passage "
            f"(boamp.parquet indisponible : {e!r})."
        )
        return None
    # Vide si boamp.parquet n'a pas encore acheteur_codePostal.
    logger.info(f"{code} : dictionnaire SIRET de {dictionnaire.height} entrées.")
    return dictionnaire


def dictionnaire_partage(today: date) -> pl.DataFrame | None:
    cle = (today, str(config.DEAO_DATA_DIR))
    with _VERROU_DICTIONNAIRE:
        if cle not in _DICTIONNAIRES:
            _DICTIONNAIRES.clear()
            dossier = config.DEAO_DATA_DIR / "sources" / "_dictionnaire_siret"
            dossier.mkdir(parents=True, exist_ok=True)
            _DICTIONNAIRES[cle] = _dictionnaire("plateformes", dossier)
        return _DICTIONNAIRES[cle]


def _conformer(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col(nom).cast(dtype)
        if nom in df.columns
        else pl.lit(None, dtype=dtype).alias(nom)
        for nom, dtype in published_schema().items()
    )


def instant_du_passage(today: date, maintenant: datetime | None = None) -> datetime:
    """Instant auquel la liste est lue : maintenant pour un passage du jour, fin de la
    journée `today` pour un `run_date` explicite."""
    maintenant = (maintenant or datetime.now(FUSEAU)).astimezone(FUSEAU)
    if maintenant.date() == today:
        return maintenant
    return _fin_de_journee(today)


def _fin_de_journee(jour: date) -> datetime:
    return datetime.combine(jour, time.max, tzinfo=FUSEAU)


def annulations_du_passage(
    precedent: pl.DataFrame, fusionne: pl.DataFrame
) -> tuple[int, int]:
    """(consultations ouvertes connues passées à « annule », consultations ouvertes
    connues) : sert à repérer une annulation massive suspecte."""
    if not precedent.height:
        return 0, 0
    ouvertes = precedent.filter(pl.col("statut") == "ouvert")["id"].unique()
    annulees = fusionne.filter(
        (pl.col("statut") == "annule") & pl.col("id").is_in(ouvertes.implode())
    )["id"].n_unique()
    return annulees, ouvertes.len()


def fusionner(
    precedent: pl.DataFrame,
    nouvelles: pl.DataFrame,
    ids_vus: set[str],
    today: date,
    complete: bool,
    delai_jours: int = DEAO_STATUT_DELAI_JOURS,
    instant: datetime | None = None,
) -> pl.DataFrame:
    """Remplace les lignes des consultations relues, garde les autres et recalcule le statut.

    `instant` : moment de la lecture de la liste (défaut : fin de la journée `today`) ;
    une date limite passée à cet instant, même plus tôt dans la journée, clôt la
    consultation au lieu de l'annuler."""
    if instant is None:
        instant = _fin_de_journee(today)
    precedent = _conformer(precedent)
    nouvelles = _conformer(nouvelles)
    ids_nouveaux = nouvelles["id"].to_list()
    gardees = precedent.filter(~pl.col("id").is_in(ids_nouveaux))
    df = pl.concat(
        [gardees, nouvelles.with_columns(pl.lit(None, pl.String).alias("statut"))]
    )
    # statut précédent par consultation (celui des lignes remplacées compte aussi)
    statut_prec = precedent.group_by("id").agg(
        pl.col("statut").first().alias("statut_prec")
    )
    df = df.join(statut_prec, on="id", how="left")
    # Pour les lignes gardées, `statut` est déjà le statut précédent.
    prec = pl.coalesce(pl.col("statut_prec"), pl.col("statut"))
    limite = pl.col("dateLimiteReponse")
    echue = limite <= pl.lit(
        instant.astimezone(FUSEAU), dtype=df.schema["dateLimiteReponse"]
    )
    vu = pl.col("id").is_in(list(ids_vus))
    # Jamais « annule » sans date limite : dans le doute, on n'annule pas.
    annule = (pl.lit(complete) & limite.is_not_null() & ~echue & (prec != "clos")) | (
        prec == "annule"
    )
    df = df.with_columns(
        pl.when(~vu & annule.fill_null(False))
        .then(pl.lit("annule"))
        .when(echue.fill_null(False))
        .then(pl.lit("clos"))
        .otherwise(statut_temporel_expr(today, delai_jours))
        .alias("statut")
    )
    # `uid` doit rester unique (une consultation lue deux fois pendant la pagination).
    df = df.unique("uid", keep="last", maintain_order=True)
    return _conformer(df.drop("statut_prec")).sort("uid")

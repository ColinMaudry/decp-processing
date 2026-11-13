"""Scraper d'une plateforme Atexo MPE : un passage complet, au contrat `Scraper`.

Un passage relit le fichier publié de la source (accumulation), lit toutes les
consultations en cours et leur page détail, relit les lots des seules consultations
nouvelles ou modifiées, archive les DCE dans S3 (si la publication est autorisée),
enrichit les acheteurs puis fusionne avec les consultations déjà connues.
"""

import shutil
import threading
from collections import Counter
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl

from src import config
from src.config import SourceDeao
from src.flows.sirene_preprocess import sirene_preprocess
from src.tasks.deao import referentiels
from src.tasks.deao.enrich import dictionnaire_depuis_publie, enrich_acheteurs
from src.tasks.deao.geocode_cache import geocoder_adresses
from src.tasks.deao.schema import published_schema, selection_publiee
from src.tasks.deao.sources_publiees import (
    FichierSourceIndisponible,
    publication_autorisee,
    telecharger_fichier_source,
)
from src.tasks.scrap.deao.atexo.accumulation import (
    dce_a_telecharger,
    fusionner,
    lots_a_relire,
    recherche_complete,
)
from src.tasks.scrap.deao.atexo.client import ClientAtexo, RechercheIncomplete
from src.tasks.scrap.deao.atexo.dce import (
    cle_dce,
    cle_depuis_url,
    deposer_dce,
    taille_archivee,
)
from src.tasks.scrap.deao.atexo.lignes import (
    lignes_consultation,
    lots_depuis_precedent,
)
from src.tasks.scrap.deao.atexo.parse import ConsultationListe, DetailConsultation
from src.tasks.utils import get_logger

# Les références de module (charger_referentiels, geocoder_adresses,
# sirene_preprocess) et config.* sont lues à l'appel : les tests les substituent.

# Colonnes des lignes avant enrichissement : schéma publié + adresse de l'acheteur.
_SCHEMA_LIGNES = {
    **published_schema(),
    **{
        c: pl.String
        for c in (
            "acheteur_adresse_voie",
            "acheteur_adresse_cp",
            "acheteur_adresse_ville",
            "acheteur_adresse_nuts",
            "acheteur_pays",
        )
    },
}
_COLONNES_DICTIONNAIRE = ("acheteur_nom", "acheteur_id", "acheteur_codePostal")

# Les deux plateformes tournent en parallèle : un seul prétraitement SIRENE.
_VERROU_SIRENE = threading.Lock()


def charger_referentiels() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    return referentiels.charger_referentiels(config.SIRENE_DATA_DIR, config.DATA_DIR)


def _referentiels() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    """Référentiels d'enrichissement ; SIRENE du mois absent : prétraitement, comme
    scrap_open."""
    with _VERROU_SIRENE:
        if not config.SIRENE_DATA_DIR.exists():
            get_logger(level=config.LOG_LEVEL).info(
                f"{config.SIRENE_DATA_DIR} absent, lancement de sirene_preprocess..."
            )
            sirene_preprocess()
    return charger_referentiels()


def _lire_precedent(entree: SourceDeao, dossier: Path) -> pl.DataFrame:
    """Fichier publié de la source ; absent : départ de zéro seulement sur demande
    explicite (une panne de data.gouv.fr ne doit pas écraser l'historique)."""
    try:
        chemin = telecharger_fichier_source(entree, dossier)
    except FichierSourceIndisponible as e:
        if not config.DEAO_SCRAP_PARTIR_DE_ZERO:
            raise
        get_logger(level=config.LOG_LEVEL).warning(
            f"{entree.code} : fichier publié indisponible ({e}), départ de zéro "
            "(DEAO_SCRAP_PARTIR_DE_ZERO)."
        )
        return pl.DataFrame(schema=published_schema())
    return selection_publiee(pl.read_parquet(chemin))


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
        return dictionnaire_depuis_publie(pl.read_parquet(chemin, columns=colonnes))
    except Exception as e:
        logger.warning(
            f"{code} : pas de dictionnaire SIRET pour ce passage "
            f"(boamp.parquet indisponible : {e!r})."
        )
        return None


def _archiver_dce(
    client: ClientAtexo,
    code: str,
    c: ConsultationListe,
    detail: DetailConsultation,
    url_precedente: str | None,
    dossier_tmp: Path,
    stats: Counter,
) -> str | None:
    """URL du DCE archivé : déposé s'il est nouveau ou si sa taille affichée a changé,
    sinon celui du passage précédent. La clé d'un DCE déjà archivé est conservée."""
    cle_precedente = cle_depuis_url(url_precedente)
    archivee = taille_archivee(cle_precedente) if cle_precedente else None
    if not dce_a_telecharger(detail.taille_dce, archivee, url_precedente):
        return url_precedente
    telecharge = client.telecharger_dce(c.ref_cons, c.org_cons, dossier_tmp)
    cle = cle_precedente or cle_dce(
        code, telecharge.id_dce or c.ref_cons, telecharge.nom_fichier
    )
    taille = telecharge.chemin.stat().st_size
    url = deposer_dce(telecharge, cle, detail.taille_dce)
    stats["dce_deposes"] += 1
    stats["dce_octets"] += taille
    return url


def _collecter(
    client: ClientAtexo,
    entree: SourceDeao,
    precedent: pl.DataFrame,
    dossier_tmp: Path,
    stats: Counter,
) -> tuple[list[ConsultationListe], int | None, list[dict]]:
    """Consultations lues, nombre annoncé (None si inconnu) et lignes rafraîchies."""
    code = entree.code
    logger = get_logger(level=config.LOG_LEVEL)
    try:
        consultations = client.rechercher()
        annoncees = len(consultations)
    except RechercheIncomplete as e:
        logger.warning(f"{code} : {e} ; aucune consultation ne sera annulée.")
        consultations, annoncees = e.consultations, e.annoncees
        stats["recherche_incomplete"] += 1

    connus = set(precedent["id"].to_list())
    urls_dce = dict(
        precedent.filter(pl.col("urlDce").is_not_null())
        .group_by("id")
        .agg(pl.col("urlDce").first())
        .iter_rows()
    )
    a_relire = lots_a_relire(consultations, precedent)
    archiver = publication_autorisee() and bool(config.S3_BUCKET)
    lignes: list[dict] = []
    for c in consultations:
        try:
            detail = client.detail(c.ref_cons, c.org_cons)
            if c.nombre_lots > 0 and c.ref_cons in a_relire:
                lots = client.lots(c.ref_cons, c.org_cons)
            elif c.nombre_lots > 0:
                lots = lots_depuis_precedent(precedent, c.ref_cons)
            else:
                lots = []
        except Exception as e:
            stats["detail_en_erreur"] += 1
            suite = (
                "lignes précédentes conservées"
                if c.ref_cons in connus
                else "consultation ignorée ce passage"
            )
            logger.warning(f"{code} {c.ref_cons} : détail en erreur ({e!r}), {suite}.")
            continue
        stats["rafraichies"] += 1

        url_dce = urls_dce.get(c.ref_cons)
        if archiver:
            try:
                url_dce = _archiver_dce(
                    client, code, c, detail, url_dce, dossier_tmp, stats
                )
            except Exception as e:
                # On garde l'éventuelle archive précédente ; nouvel essai au passage suivant.
                stats["dce_en_erreur"] += 1
                logger.warning(f"{code} {c.ref_cons} : DCE en erreur ({e!r}).")
        lignes.extend(
            lignes_consultation(code, entree.url_base, c, detail, lots, url_dce, stats)
        )
    return consultations, annoncees, lignes


def _passage(
    code: str, today: date, client_factory: Callable[[str], ClientAtexo]
) -> pl.DataFrame:
    logger = get_logger(level=config.LOG_LEVEL)
    entree = next((e for e in config.DEAO_REGISTRE if e.code == code), None)
    if entree is None or not entree.url_base:
        raise ValueError(f"{code} : entrée de registre avec url_base requise.")

    # Dossier propre à la source : les deux plateformes téléchargent boamp.parquet en parallèle.
    dossier_sources = config.DEAO_DATA_DIR / "sources" / code
    dossier_sources.mkdir(parents=True, exist_ok=True)
    precedent = _lire_precedent(entree, dossier_sources)
    dictionnaire = _dictionnaire(code, dossier_sources)
    lf_unites_legales, lf_etablissements, df_cog = _referentiels()

    stats: Counter = Counter()
    dossier_tmp = config.DEAO_DATA_DIR / "dce_tmp" / code
    shutil.rmtree(dossier_tmp, ignore_errors=True)
    dossier_tmp.mkdir(parents=True)
    client = client_factory(entree.url_base)
    try:
        consultations, annoncees, lignes = _collecter(
            client, entree, precedent, dossier_tmp, stats
        )
    finally:
        client.close()
        # Un téléchargement interrompu peut laisser un fichier partiel.
        shutil.rmtree(dossier_tmp, ignore_errors=True)

    complete = not stats["recherche_incomplete"] and recherche_complete(
        len(consultations),
        annoncees,
        precedent,
        today,
        config.DEAO_STATUT_DELAI_JOURS,
    )
    nouvelles = pl.DataFrame(lignes, schema=_SCHEMA_LIGNES, strict=False)
    if nouvelles.height:
        nouvelles = enrich_acheteurs(
            nouvelles,
            lf_unites_legales,
            lf_etablissements,
            df_cog,
            geocoder=referentiels.geocoder_tolerant(today, stats, geocoder_adresses),
            dictionnaire=dictionnaire,
        )
    df = fusionner(
        precedent,
        selection_publiee(nouvelles),
        {c.ref_cons for c in consultations},
        today,
        complete,
        config.DEAO_STATUT_DELAI_JOURS,
    )

    inconnues = {
        k.removeprefix("procedure_inconnue:"): v
        for k, v in stats.items()
        if k.startswith("procedure_inconnue:")
    }
    avec_siret = df["acheteur_id"].is_not_null().sum()
    logger.info(
        f"{code} : {len(consultations)} consultations lues / {annoncees} annoncées"
        f"{'' if complete else ' (recherche incomplète ou trop courte : aucune annulation)'}"
        f", {stats['rafraichies']} rafraîchies ; {stats['dce_deposes']} DCE déposés "
        f"({stats['dce_octets'] / 1e6:.1f} Mo) ; erreurs : {stats['detail_en_erreur']} "
        f"détail, {stats['dce_en_erreur']} DCE, {stats['geocodage_en_erreur']} géocodage ; "
        f"procédures inconnues : {inconnues or 'aucune'} ; {df.height} lignes, "
        f"{df['id'].n_unique()} consultations, {avec_siret} lignes avec acheteur_id."
    )
    return df


def scraper_atexo(
    code: str, client_factory: Callable[[str], ClientAtexo] = ClientAtexo
) -> Callable[[date], pl.DataFrame]:
    """Scraper de la plateforme `code` du registre (aucun accès au registre, au réseau
    ni à S3 avant l'appel)."""

    def scraper(today: date) -> pl.DataFrame:
        return _passage(code, today, client_factory)

    return scraper

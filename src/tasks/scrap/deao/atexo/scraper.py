"""Scraper d'une plateforme Atexo MPE : un passage complet, au contrat `Scraper`.

Un passage relit le fichier publié de la source (accumulation), lit toutes les
consultations en cours et leur page détail, relit les lots des seules consultations
nouvelles ou modifiées, archive les DCE dans S3 (si la publication est autorisée),
enrichit les acheteurs puis fusionne avec les consultations déjà connues.
"""

import shutil
from collections import Counter
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl

from src import config
from src.config import SourceDeao
from src.tasks.deao import referentiels
from src.tasks.deao.enrich import enrich_acheteurs
from src.tasks.deao.schema import published_schema, selection_publiee
from src.tasks.deao.sources_publiees import publication_autorisee
from src.tasks.scrap.deao import plateformes
from src.tasks.scrap.deao.atexo.accumulation import (
    dce_a_telecharger,
    lots_a_relire,
    recherche_complete,
)
from src.tasks.scrap.deao.atexo.client import ClientAtexo, RechercheIncomplete
from src.tasks.scrap.deao.atexo.dce import (
    ErreurDepotS3,
    cle_dce,
    cle_depuis_url,
    deposer_dce,
    taille_archivee,
    verifier_depot_s3,
)
from src.tasks.scrap.deao.atexo.lignes import (
    lignes_consultation,
    lots_depuis_precedent,
)
from src.tasks.scrap.deao.atexo.parse import ConsultationListe, DetailConsultation
from src.tasks.scrap.deao.erreurs import ScrapPartiel
from src.tasks.scrap.deao.plateformes import (
    SEUIL_ANNULATIONS,
    annulations_du_passage,
    dictionnaire_partage,
    fusionner,
    instant_du_passage,
    lire_precedent,
    referentiels_enrichissement,
)
from src.tasks.utils import get_logger

# `plateformes.geocoder_adresses` et config.* sont lus à l'appel : les tests les
# substituent (charger_referentiels et sirene_preprocess : voir `plateformes`).

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


def _suite(c: ConsultationListe, connus: set[str]) -> str:
    """Sort d'une consultation en erreur : une connue garde ses lignes précédentes."""
    if c.ref_cons in connus:
        return "lignes précédentes conservées"
    return "consultation ignorée ce passage"


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

    def couper_archivage(e: ErreurDepotS3) -> None:
        # Coupe-circuit : sans dépôt possible, chaque DCE serait retéléchargé à
        # chaque passage. Les URL précédentes sont gardées ; le passage échouera.
        nonlocal archiver
        archiver = False
        stats[f"s3_en_erreur:{e}"] += 1
        logger.error(f"{code} : {e} ; archivage des DCE arrêté pour ce passage.")

    if archiver:
        try:
            verifier_depot_s3(code)
        except ErreurDepotS3 as e:
            couper_archivage(e)
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
            logger.warning(
                f"{code} {c.ref_cons} : détail en erreur ({e!r}), {_suite(c, connus)}."
            )
            continue

        url_dce = urls_dce.get(c.ref_cons)
        if archiver:
            try:
                url_dce = _archiver_dce(
                    client, code, c, detail, url_dce, dossier_tmp, stats
                )
            except ErreurDepotS3 as e:
                couper_archivage(e)
            except Exception as e:
                # On garde l'éventuelle archive précédente ; nouvel essai au passage suivant.
                stats["dce_en_erreur"] += 1
                logger.warning(f"{code} {c.ref_cons} : DCE en erreur ({e!r}).")
        try:
            lignes.extend(
                lignes_consultation(
                    code, entree.url_base, c, detail, lots, url_dce, stats
                )
            )
        except Exception as e:
            stats["lignes_en_erreur"] += 1
            logger.warning(
                f"{code} {c.ref_cons} : lignes en erreur ({e!r}), {_suite(c, connus)}."
            )
            continue
        stats["rafraichies"] += 1
    return consultations, annoncees, lignes


def _passage(
    code: str, today: date, client_factory: Callable[[str], ClientAtexo]
) -> pl.DataFrame:
    logger = get_logger(level=config.LOG_LEVEL)
    entree = next((e for e in config.DEAO_REGISTRE if e.code == code), None)
    if entree is None or not entree.url_base:
        raise ValueError(f"{code} : entrée de registre avec url_base requise.")

    # Dossier propre à la source : les plateformes téléchargent en parallèle.
    dossier_sources = config.DEAO_DATA_DIR / "sources" / code
    dossier_sources.mkdir(parents=True, exist_ok=True)
    precedent = lire_precedent(entree, dossier_sources)
    dictionnaire = dictionnaire_partage(today)
    lf_unites_legales, lf_etablissements, df_cog = referentiels_enrichissement()

    stats: Counter = Counter()
    dossier_tmp = config.DEAO_DATA_DIR / "dce_tmp" / code
    shutil.rmtree(dossier_tmp, ignore_errors=True)
    dossier_tmp.mkdir(parents=True)
    client = client_factory(entree.url_base)
    # Instant de lecture de la liste : une date limite passée à cet instant clôt.
    instant = instant_du_passage(today)
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
            geocoder=referentiels.geocoder_tolerant(
                today, stats, plateformes.geocoder_adresses
            ),
            dictionnaire=dictionnaire,
        )
    df = fusionner(
        precedent,
        selection_publiee(nouvelles),
        {c.ref_cons for c in consultations},
        today,
        complete,
        config.DEAO_STATUT_DELAI_JOURS,
        instant,
    )

    annulees, ouvertes = annulations_du_passage(precedent, df)
    if ouvertes and annulees > SEUIL_ANNULATIONS * ouvertes:
        logger.warning(
            f"{code} : {annulees} consultations ouvertes sur {ouvertes} annulées "
            f"en un passage (plus de {SEUIL_ANNULATIONS:.0%}), à vérifier."
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
        f"détail, {stats['lignes_en_erreur']} lignes, {stats['dce_en_erreur']} DCE, {stats['geocodage_en_erreur']} géocodage ; "
        f"procédures inconnues : {inconnues or 'aucune'} ; {df.height} lignes, "
        f"{df['id'].n_unique()} consultations, {avec_siret} lignes avec acheteur_id."
    )
    erreurs_s3 = [
        k.removeprefix("s3_en_erreur:") for k in stats if k.startswith("s3_en_erreur:")
    ]
    if erreurs_s3:
        # Les lignes sont publiées, mais le flow doit signaler l'échec.
        raise ScrapPartiel(
            f"dépôt S3 des DCE en échec, archivage interrompu ({erreurs_s3[0]})", df
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

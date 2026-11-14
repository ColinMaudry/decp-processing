"""Scraper de www.marches-publics.info (AWS) : un passage complet, au contrat `Scraper`.

Un passage relit le fichier publié de la source (accumulation), lit la liste de tous
les avis en cours, puis ne relit (page d'avis et RC) que les consultations nouvelles
ou dont la date limite ou le nombre de lots a changé (relecture B). Les pages d'avis
sont archivées dans S3 (privées) et servent de cache ; les RC y sont déposés en
lecture publique. Les acheteurs sont enrichis, puis les lignes fusionnées avec les
consultations déjà connues.
"""

import shutil
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src import config
from src.config import SourceDeao
from src.tasks.deao import referentiels
from src.tasks.deao.enrich import enrich_acheteurs
from src.tasks.deao.schema import published_schema, selection_publiee
from src.tasks.deao.sources_publiees import publication_autorisee
from src.tasks.scrap.deao import plateformes
from src.tasks.scrap.deao.atexo.dce import ErreurDepotS3, verifier_depot_s3
from src.tasks.scrap.deao.aws.archive import (
    cle_rc,
    deposer_avis,
    deposer_rc,
    lire_avis_archive,
    marque_date_limite,
)
from src.tasks.scrap.deao.aws.client import (
    ClientAws,
    ResultatRecherche,
    url_rc_plateforme,
)
from src.tasks.scrap.deao.aws.lignes import lignes_avis
from src.tasks.scrap.deao.aws.parse import Avis, AvisListe, parse_avis
from src.tasks.scrap.deao.erreurs import ScrapPartiel
from src.tasks.scrap.deao.plateformes import (
    FUSEAU,
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
# Schéma explicite : une colonne nulle sur toutes les lignes ne doit pas être inférée.
_SCHEMA_LIGNES = {
    **published_schema(),
    **{
        c: pl.String
        for c in (
            "acheteur_adresse_voie",
            "acheteur_adresse_cp",
            "acheteur_adresse_ville",
            # Attendue par enrich_acheteurs ; nulle ici (la page d'avis ne donne que
            # le NUTS du lieu d'exécution, pas celui de l'adresse de l'acheteur).
            "acheteur_adresse_nuts",
            "acheteur_pays",
        )
    },
}


def _marque(d: datetime | None) -> str:
    """Date limite à la minute, dans le fuseau de Paris (comparaison et métadonnée S3)."""
    if d is not None and d.tzinfo is not None:
        d = d.astimezone(FUSEAU)
    return marque_date_limite(d)


def _etat_precedent(precedent: pl.DataFrame) -> dict[str, tuple[str, int, str | None]]:
    """Par consultation du fichier précédent : date limite (à la minute), nombre de
    lots (lignes `lotId` != « 0 ») et URL du RC."""
    if not precedent.height:
        return {}
    agrege = precedent.group_by("id").agg(
        pl.col("dateLimiteReponse").drop_nulls().first(),
        (pl.col("lotId") != "0").fill_null(True).sum().alias("lots"),
        pl.col("urlReglementConsultation").drop_nulls().first(),
    )
    return {
        idm: (_marque(limite), lots, url)
        for idm, limite, lots, url in agrege.select(
            "id", "dateLimiteReponse", "lots", "urlReglementConsultation"
        ).iter_rows()
    }


def a_relire(
    avis: list[AvisListe], etat: dict[str, tuple[str, int, str | None]]
) -> set[str]:
    """IDM nouveaux, ou dont la date limite (à la minute) ou le nombre de lots diffère
    du fichier précédent. Une date limite absente de la liste n'est pas comparée."""
    relire = set()
    for a in avis:
        if a.idm not in etat:
            relire.add(a.idm)
            continue
        limite, lots, _ = etat[a.idm]
        if (a.date_limite is not None and _marque(a.date_limite) != limite) or (
            lots != a.nombre_lots
        ):
            relire.add(a.idm)
    return relire


class _Passage:
    """État d'un passage : client, archivage S3 (coupe-circuit) et compteurs."""

    def __init__(
        self,
        client: ClientAws,
        entree: SourceDeao,
        dossier_tmp: Path,
        stats: Counter,
    ):
        self.client = client
        self.code = entree.code
        self.url_base = entree.url_base
        self.dossier_tmp = dossier_tmp
        self.stats = stats
        self.logger = get_logger(level=config.LOG_LEVEL)
        self.archiver = publication_autorisee() and bool(config.S3_BUCKET)
        if self.archiver:
            try:
                verifier_depot_s3(self.code)
            except ErreurDepotS3 as e:
                self.couper_archivage(e)

    def couper_archivage(self, e: ErreurDepotS3) -> None:
        # Coupe-circuit : sans S3, chaque page et chaque RC seraient relus à chaque
        # passage. Les pages sont lues sur le site ; le passage échouera.
        self.archiver = False
        self.stats[f"s3_en_erreur:{e}"] += 1
        self.logger.error(
            f"{self.code} : {e} ; archivage des pages d'avis et des RC arrêté "
            "pour ce passage."
        )

    def _archive(self, idm: str, marque: str | None) -> Avis | None:
        """Avis lu dans l'archive S3 ; None si absent, modifié ou illisible."""
        if not self.archiver:
            return None
        try:
            html = lire_avis_archive(idm, marque)
        except ErreurDepotS3 as e:
            self.couper_archivage(e)
            return None
        if html is None:
            return None
        try:
            return parse_avis(html)
        except ValueError as e:
            self.stats["archive_illisible"] += 1
            self.logger.warning(
                f"{self.code} {idm} : page archivée illisible ({e}), relue sur le site."
            )
            return None

    def _site(self, a: AvisListe, marque: str) -> Avis:
        """Avis lu sur le site, puis archivé (si l'archivage est actif)."""
        html = self.client.page_avis(a.chemin_avis)
        self.stats["pages_site"] += 1
        avis = parse_avis(html)
        if self.archiver:
            try:
                deposer_avis(a.idm, html, marque)
                self.stats["avis_deposes"] += 1
            except ErreurDepotS3 as e:
                self.couper_archivage(e)
        return avis

    def avis(self, a: AvisListe, relue: bool) -> Avis:
        """Consultation à relire : cache S3 à même date limite et même nombre de lots
        que la liste, sinon site. Reconstruction (DEAO_AWS_RELIRE_ARCHIVE) : archive
        quelle que soit la date limite, sinon site."""
        marque = _marque(a.date_limite)
        avis = self._archive(a.idm, marque if relue else None)
        if avis is not None and relue and len(avis.lots) != a.nombre_lots:
            # Lots modifiés à date limite inchangée : la page archivée est périmée.
            avis = None
        if avis is not None:
            self.stats["pages_cache" if relue else "pages_archive"] += 1
            return avis
        return self._site(a, marque)

    def rc(self, a: AvisListe, url_precedente: str | None, relue: bool) -> str:
        """URL du RC : déposé dans S3 pour une consultation relue (lien plateforme en
        cas d'échec) ; sinon celle du passage précédent, à défaut le lien plateforme."""
        lien = url_rc_plateforme(self.url_base, a.idm)
        if not relue or not self.archiver:
            return url_precedente or lien
        try:
            chemin = self.client.telecharger_rc(a.idm, self.dossier_tmp)
            url = deposer_rc(chemin, cle_rc(self.code, a.idm))
            self.stats["rc_deposes"] += 1
            return url
        except ErreurDepotS3 as e:
            self.couper_archivage(e)
        except Exception as e:
            self.stats["rc_en_erreur"] += 1
            self.logger.warning(f"{self.code} {a.idm} : RC en erreur ({e!r}).")
        return lien


def _suite(idm: str, connus: dict) -> str:
    """Sort d'une consultation en erreur : une connue garde ses lignes précédentes."""
    if idm in connus:
        return "lignes précédentes conservées"
    return "consultation ignorée ce passage"


def _collecter(
    client: ClientAws,
    entree: SourceDeao,
    precedent: pl.DataFrame,
    dossier_tmp: Path,
    stats: Counter,
) -> tuple[ResultatRecherche, list[dict]]:
    """Résultat de la recherche et lignes des consultations relues ou reconstruites."""
    code = entree.code
    logger = get_logger(level=config.LOG_LEVEL)
    resultat = client.rechercher()
    etat = _etat_precedent(precedent)
    relire = a_relire(resultat.avis, etat)
    passage = _Passage(client, entree, dossier_tmp, stats)
    reconstruire = config.DEAO_AWS_RELIRE_ARCHIVE
    if reconstruire and not passage.archiver:
        logger.warning(
            f"{code} : DEAO_AWS_RELIRE_ARCHIVE ignoré, archive S3 inaccessible ou "
            "publication non autorisée."
        )

    lignes: list[dict] = []
    for a in resultat.avis:
        relue = a.idm in relire
        # Reconstruction : seulement tant que l'archive est accessible.
        if not relue and not (reconstruire and passage.archiver):
            continue
        try:
            avis = passage.avis(a, relue)
        except Exception as e:
            stats["avis_en_erreur"] += 1
            logger.warning(
                f"{code} {a.idm} : page d'avis en erreur ({e!r}), {_suite(a.idm, etat)}."
            )
            continue
        if relue:
            stats["relues"] += 1
        else:
            stats["reconstruites"] += 1
        url_precedente = etat[a.idm][2] if a.idm in etat else None
        url_rc = passage.rc(a, url_precedente, relue)
        try:
            lignes.extend(lignes_avis(code, entree.url_base, a, avis, url_rc, stats))
        except Exception as e:
            stats["lignes_en_erreur"] += 1
            logger.warning(
                f"{code} {a.idm} : lignes en erreur ({e!r}), {_suite(a.idm, etat)}."
            )
            continue
        stats["rafraichies"] += 1
    return resultat, lignes


def _passage(
    code: str, today: date, client_factory: Callable[[str], ClientAws]
) -> pl.DataFrame:
    logger = get_logger(level=config.LOG_LEVEL)
    entree = next((e for e in config.DEAO_REGISTRE if e.code == code), None)
    if entree is None or not entree.url_base:
        raise ValueError(f"{code} : entrée de registre avec url_base requise.")

    dossier_sources = config.DEAO_DATA_DIR / "sources" / code
    dossier_sources.mkdir(parents=True, exist_ok=True)
    precedent = lire_precedent(entree, dossier_sources)
    dictionnaire = dictionnaire_partage(today)
    lf_unites_legales, lf_etablissements, df_cog = referentiels_enrichissement()

    stats: Counter = Counter()
    dossier_tmp = config.DEAO_DATA_DIR / "rc_tmp" / code
    shutil.rmtree(dossier_tmp, ignore_errors=True)
    dossier_tmp.mkdir(parents=True)
    client = client_factory(entree.url_base)
    # Instant de lecture de la liste : une date limite passée à cet instant clôt.
    instant = instant_du_passage(today)
    try:
        resultat, lignes = _collecter(client, entree, precedent, dossier_tmp, stats)
    finally:
        client.close()
        # Un téléchargement interrompu peut laisser un fichier partiel.
        shutil.rmtree(dossier_tmp, ignore_errors=True)

    complete = resultat.complete
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
        {a.idm for a in resultat.avis},
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
        f"{code} : {len(resultat.avis)} consultations lues (IDM), "
        f"{resultat.hors_plateforme} hors plateforme, total {resultat.total_avant} avant / "
        f"{resultat.total_apres} après, {len(resultat.segments_en_erreur)} segments "
        f"en erreur, recherche "
        f"{'complète' if complete else 'incomplète (aucune annulation)'} ; "
        f"{stats['relues']} relues, {stats['reconstruites']} reconstruites depuis "
        f"l'archive ; pages : {stats['pages_cache']} cache S3, "
        f"{stats['pages_archive']} archive, {stats['pages_site']} site, "
        f"{stats['avis_deposes']} déposées ; RC : {stats['rc_deposes']} déposés, "
        f"{stats['rc_en_erreur']} en erreur ; erreurs : {stats['avis_en_erreur']} "
        f"avis, {stats['archive_illisible']} archive illisible, "
        f"{stats['lignes_en_erreur']} lignes, {stats['geocodage_en_erreur']} "
        f"géocodage ; procédures inconnues : {inconnues or 'aucune'} ; "
        f"{df.height} lignes, {df['id'].n_unique()} consultations, {avec_siret} "
        "lignes avec acheteur_id."
    )
    erreurs_s3 = [
        k.removeprefix("s3_en_erreur:") for k in stats if k.startswith("s3_en_erreur:")
    ]
    if erreurs_s3:
        # Les lignes sont publiées, mais le flow doit signaler l'échec.
        raise ScrapPartiel(
            "accès S3 des pages d'avis et des RC en échec, archivage interrompu "
            f"({erreurs_s3[0]})",
            df,
        )
    return df


def scraper_aws(
    code: str, client_factory: Callable[[str], ClientAws] = ClientAws
) -> Callable[[date], pl.DataFrame]:
    """Scraper de la plateforme AWS `code` du registre (aucun accès au registre, au
    réseau ni à S3 avant l'appel)."""

    def scraper(today: date) -> pl.DataFrame:
        return _passage(code, today, client_factory)

    return scraper

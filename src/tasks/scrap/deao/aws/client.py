"""Client HTTP de www.marches-publics.info (AWS) : recherche, page d'avis, RC.

Recherche : `POST /Annonces/lister` (formulaire de `/Annonces/rechercher`), puis
pagination en GET `/Annonces/lister?pager_{t|s|f}=N`, qui dépend de la dernière
recherche de la session : tout est séquentiel sur un même client.

Le site refuse d'afficher plus de ~100 résultats (« Votre recherche trouve N
résultats ») et sa pagination est peu fiable (404, page vide ou formulaire de
recherche à la place de la page N). Stratégie :

1. un POST par département ; trop large -> un POST par nature (T, S, F) ; trop large
   -> un POST par procédure ;
2. pour un onglet dont le badge dépasse 10, les pages 2 à N sont lues juste après le
   POST, sans autre requête entre-temps ; une page 404, vide ou « formulaire » est une
   page manquante, jamais une erreur fatale ;
3. onglet encore incomplet : un nouveau POST du même segment et les pages de nouveau ;
4. toujours incomplet : découpage par nature (segment département), puis par
   procédure (segment nature) ; un segment nature × procédure incomplet est noté dans
   `segments_en_erreur` (`{dép}/{nature}/{procédure}`) et la collecte continue.

Jamais de requête vers les URL du DCE (`dematEnt`, captcha).
"""

import logging
from dataclasses import dataclass, field
from math import ceil
from pathlib import Path

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src import config
from src.tasks.scrap.deao.aws.parse import (
    AvisListe,
    aucun_resultat,
    badges,
    departements,
    numeros_avis,
    parse_liste,
    trop_de_resultats,
)
from src.tasks.scrap.deao.limiteur import Limiteur

logger = logging.getLogger(__name__)

RESULTATS_PAR_PAGE = 10
# Le site renvoie parfois le formulaire à la place de la page N (plusieurs serveurs
# sans session partagée) : la même URL réussit en général après quelques essais.
ESSAIS_PAGE = 8
# Part minimale du total national que la lecture doit atteindre : chaque segment est
# déjà contrôlé sur son badge, mais quelques avis (département `00`, hors formulaire)
# n'apparaissent dans aucune recherche par département.
PART_MINIMALE_TOTAL = 0.9
# Département absent du formulaire qui renvoie pourtant des avis
DEPARTEMENT_SUPPLEMENTAIRE = "00"
NATURES = ("T", "S", "F")
PROCEDURES = ("PA", "AOO", "AOE", "MN", "CO", "CR", "DC", "AU", "DSP")

_CHAMPS_VIDES = (
    "txtLibre",
    "txtLibreLieuExec",
    "dateNotifDebut",
    "dateNotifFin",
    "txtAcheteurNom",
    "txtAcheteurSiret",
    "txtTitulaireNom",
    "txtTitulaireSiret",
    "txtLibreAcheteur",
    "txtLibreVille",
    "txtLibreRef",
    "txtLibreObjet",
    "dateExpiration",
    "dateParution",
)


@dataclass
class ResultatRecherche:
    """Avis lus (avec IDM, dédoublonnés sur `idm`) et état de complétude.

    `complete` : aucun segment en erreur, totaux nationaux connus et avis distincts lus
    (avec IDM + `hors_plateforme`) au moins `PART_MINIMALE_TOTAL` du plus petit total.
    """

    avis: list[AvisListe]
    total_avant: int | None
    total_apres: int | None
    segments_en_erreur: list[str] = field(default_factory=list)
    # Avis distincts lus sans ligne produite (sans IDM, dépôt ailleurs, autre
    # plateforme AWS comme `DL`) : ils comptent pour la complétude mais n'ont pas de RC AWS.
    hors_plateforme: int = 0

    @property
    def complete(self) -> bool:
        if self.segments_en_erreur:
            return False
        if self.total_avant is None or self.total_apres is None:
            return False
        lus = len(self.avis) + self.hors_plateforme
        return lus >= PART_MINIMALE_TOTAL * min(self.total_avant, self.total_apres)


def url_rc_plateforme(url_base: str, idm: str) -> str:
    """Lien du règlement de consultation sur la plateforme (redirige vers un PDF)."""
    return f"{url_base.rstrip('/')}/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={idm}"


def _a_reessayer(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


_retry = retry(
    retry=retry_if_exception(_a_reessayer),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=5, min=5, max=60),
    reraise=True,
)


@dataclass
class _Collecte:
    avis: dict[str, AvisListe] = field(default_factory=dict)
    numeros: set[str] = field(default_factory=set)
    erreurs: list[str] = field(default_factory=list)


class ClientAws:
    """Accès HTTP à www.marches-publics.info, sur une seule session (cookies PHP)."""

    def __init__(
        self,
        url_base: str,
        http: httpx.Client | None = None,
        limiteur: Limiteur | None = None,
    ):
        self.url_base = url_base.rstrip("/")
        self.http = http or httpx.Client(
            headers={"User-Agent": config.CLOSED_USER_AGENT},
            follow_redirects=True,
            timeout=60,
        )
        self.limiteur = limiteur or Limiteur(config.CLOSED_INTERVALLE_REQUETES_S)

    @_retry
    def _requete(self, methode: str, url: str, **kwargs) -> httpx.Response:
        """Seul accès réseau hors téléchargement de fichier ; lève sur un code ≥ 400."""
        self.limiteur.attendre()
        reponse = self.http.request(methode, url, **kwargs)
        reponse.raise_for_status()
        return reponse

    def _poster(self, idr: str, idn: str = "X", idp: str = "X") -> str:
        corps = {
            "IDE": "EC",
            "IDN": idn,
            "IDP": idp,
            "IDR": idr,
            "annee": "X",
            "Rechercher": "Rechercher",
            **dict.fromkeys(_CHAMPS_VIDES, ""),
        }
        return self._requete(
            "POST", f"{self.url_base}/Annonces/lister", data=corps
        ).text

    def _page(self, onglet: str, numero: int) -> str:
        return self._requete(
            "GET", f"{self.url_base}/Annonces/lister?pager_{onglet}={numero}"
        ).text

    def _page_avec_essais(self, onglet: str, numero: int) -> str | None:
        """Page N d'un onglet, relue jusqu'à `ESSAIS_PAGE` fois si elle manque.

        Manquante : 404, formulaire ou aucun bloc dans l'onglet. Aucune autre requête
        entre deux essais.
        """
        for essai in range(1, ESSAIS_PAGE + 1):
            try:
                page = self._page(onglet, numero)
            except httpx.HTTPError as exc:
                motif = str(exc)
            else:
                if numeros_avis(page, onglet):
                    return page
                motif = "aucun avis dans l'onglet"
            if essai < ESSAIS_PAGE:
                logger.debug(
                    "Page %s de l'onglet %s manquante (essai %s/%s) : %s",
                    numero,
                    onglet,
                    essai,
                    ESSAIS_PAGE,
                    motif,
                )
        logger.warning(
            "Page %s de l'onglet %s manquante après %s essais : %s",
            numero,
            onglet,
            ESSAIS_PAGE,
            motif,
        )
        return None

    def _total_national(self) -> int | None:
        """Nombre de résultats toutes natures et départements confondus, ou None."""
        try:
            html = self._poster("X")
        except httpx.HTTPError as exc:
            logger.warning("Total national illisible : %s", exc)
            return None
        total = trop_de_resultats(html)
        if total is not None:
            return total
        return sum(badges(html).values()) or None

    def rechercher(self) -> ResultatRecherche:
        """Tous les avis en cours, par département, nature puis procédure."""
        collecte = _Collecte()
        formulaire = self._requete("GET", f"{self.url_base}/Annonces/rechercher").text
        deps = departements(formulaire)
        if not deps:
            raise ValueError("Aucun département lu sur le formulaire de recherche")
        if DEPARTEMENT_SUPPLEMENTAIRE not in deps:
            deps.append(DEPARTEMENT_SUPPLEMENTAIRE)
        logger.info("%s départements à interroger (dont `00`)", len(deps))
        total_avant = self._total_national()
        for dep in deps:
            self._segment(collecte, dep, None, None)
        total_apres = self._total_national()
        avec_idm = {f"MPI-{a.numero_avis}" for a in collecte.avis.values()}
        return ResultatRecherche(
            avis=list(collecte.avis.values()),
            total_avant=total_avant,
            total_apres=total_apres,
            segments_en_erreur=collecte.erreurs,
            hors_plateforme=len(collecte.numeros - avec_idm),
        )

    @staticmethod
    def _nom(dep: str, nature: str | None, procedure: str | None) -> str:
        return "/".join(p for p in (dep, nature, procedure) if p)

    def _poster_segment(
        self, dep: str, nature: str | None, procedure: str | None
    ) -> str | None:
        """HTML d'un POST exploitable (badges ou « trop de résultats »), sinon None.

        Une réponse sans badge (formulaire, page d'erreur en 200, session perdue)
        n'est pas « 0 résultat » : un seul nouvel essai, puis None.
        """
        nom = self._nom(dep, nature, procedure)
        for _ in range(2):
            try:
                html = self._poster(dep, nature or "X", procedure or "X")
            except httpx.HTTPError as exc:
                logger.warning("Segment %s : %s", nom, exc)
                continue
            if (
                trop_de_resultats(html) is not None
                or badges(html)
                or aucun_resultat(html)
            ):
                return html
            logger.warning("Segment %s : réponse sans badge", nom)
        return None

    def _segment(
        self,
        collecte: _Collecte,
        dep: str,
        nature: str | None,
        procedure: str | None,
    ) -> dict[str, set[str]]:
        """Collecte un segment ; renvoie les numéros lus par onglet (enfants compris)."""
        nom = self._nom(dep, nature, procedure)
        html = self._poster_segment(dep, nature, procedure)
        if html is not None and aucun_resultat(html):
            return {}  # recherche à 0 résultat : segment valide, rien à lire
        if html is None or trop_de_resultats(html) is not None:
            # Pas de badge pour contrôler : seul le total national sert de filet
            return self._decouper(collecte, dep, nature, procedure, tous=True)

        onglets = [nature.lower()] if nature else ["t", "s", "f"]
        lus: dict[str, set[str]] = {o: set() for o in onglets}
        attendus: dict[str, int] = {}
        manquants = self._lire(collecte, html, onglets, lus, attendus)
        if manquants:
            logger.info("Segment %s incomplet (%s), nouvel essai", nom, manquants)
            try:
                html = self._poster(dep, nature or "X", procedure or "X")
            except httpx.HTTPError as exc:
                logger.warning("Segment %s, nouvel essai : %s", nom, exc)
            else:
                if badges(html):
                    manquants = self._lire(collecte, html, manquants, lus, attendus)
        if manquants:
            erreurs_avant = len(collecte.erreurs)
            enfants = self._decouper(
                collecte, dep, nature, procedure, onglets=manquants
            )
            for onglet in manquants:
                lus[onglet] |= enfants.get(onglet, set())
            incomplet = any(len(lus[o]) < attendus[o] for o in manquants)
            if incomplet and len(collecte.erreurs) == erreurs_avant:
                collecte.erreurs.append(nom)
        return lus

    def _decouper(
        self,
        collecte: _Collecte,
        dep: str,
        nature: str | None,
        procedure: str | None,
        tous: bool = False,
        onglets: list[str] | None = None,
    ) -> dict[str, set[str]]:
        """Un niveau de découpage de plus ; renvoie l'union des numéros lus."""
        lus: dict[str, set[str]] = {}
        if nature is None:
            enfants = [
                self._segment(collecte, dep, o, None)
                for o in (NATURES if tous else [o.upper() for o in onglets or []])
            ]
        elif procedure is None:
            enfants = [self._segment(collecte, dep, nature, c) for c in PROCEDURES]
        else:
            collecte.erreurs.append(self._nom(dep, nature, procedure))
            enfants = []
        for enfant in enfants:
            for onglet, numeros in enfant.items():
                lus.setdefault(onglet, set()).update(numeros)
        return lus

    def _lire(
        self,
        collecte: _Collecte,
        html: str,
        onglets: list[str],
        lus: dict[str, set[str]],
        attendus: dict[str, int],
    ) -> list[str]:
        """Lit la page 1 et les suivantes des onglets ; renvoie ceux restés incomplets.

        Pages 2 à N lues dans la foulée du POST. Une page manquante (404, vide ou
        formulaire) n'interrompt rien. `attendus` garde le plus grand badge vu par
        onglet : un nouveau POST ne peut pas faire baisser l'attendu.
        """
        compteurs = badges(html)
        incomplets: list[str] = []
        for onglet in onglets:
            attendu = max(attendus.get(onglet, 0), compteurs.get(onglet, 0))
            attendus[onglet] = attendu
            if attendu == 0:
                continue
            self._ajouter(collecte, html, onglet, lus[onglet])
            for numero in range(2, ceil(attendu / RESULTATS_PAR_PAGE) + 1):
                if len(lus[onglet]) >= attendu:
                    break
                page = self._page_avec_essais(onglet, numero)
                if page is None:
                    continue
                self._ajouter(collecte, page, onglet, lus[onglet])
            if len(lus[onglet]) < attendu:
                incomplets.append(onglet)
        return incomplets

    @staticmethod
    def _ajouter(
        collecte: _Collecte, html: str, onglet: str, lus_onglet: set[str]
    ) -> None:
        numeros = numeros_avis(html, onglet)
        lus_onglet.update(numeros)
        collecte.numeros.update(numeros)
        for avis in parse_liste(html, onglet):
            collecte.avis.setdefault(avis.idm, avis)

    def page_avis(self, chemin: str) -> str:
        """HTML de la page d'avis `/Annonces/MPI-pub-{n°}.htm`."""
        return self._requete("GET", f"{self.url_base}{chemin}").text

    @_retry
    def _telecharger(self, url: str, dest: Path) -> Path:
        self.limiteur.attendre()
        with self.http.stream("GET", url) as reponse:
            reponse.raise_for_status()
            if reponse.headers.get("content-type", "").startswith("text/html"):
                raise ValueError(f"Page HTML au lieu d'un PDF : {url}")
            with open(dest, "wb") as fichier:
                for morceau in reponse.iter_bytes():
                    fichier.write(morceau)
        return dest

    def telecharger_rc(self, idm: str, dest_dir: Path) -> Path:
        """Télécharge le RC (PDF) en `{dest_dir}/{idm}.pdf` ; ValueError si HTML."""
        return self._telecharger(
            url_rc_plateforme(self.url_base, idm), Path(dest_dir) / f"{idm}.pdf"
        )

    def close(self) -> None:
        self.http.close()

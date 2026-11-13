"""Client HTTP des plateformes Atexo MPE (PRADO) : recherche, détail, lots, DCE anonyme.

Séquences vérifiées en direct le 13/11/2026 sur Mégalis Bretagne (et la recherche sur
Maximilien). Chaque postback PRADO renvoie tous les champs du formulaire lus dans la
page précédente (dont `PRADO_PAGESTATE`), sur la même session (cookies `PHPSESSID`,
`SERVERID`), avec `PRADO_POSTBACK_TARGET` = nom du contrôle déclencheur.

Recherche (`/?page=Entreprise.EntrepriseAdvancedSearch&searchAnnCons`) :

1. GET de la page : cookies et champs du formulaire.
2. POST, cible `ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche`, avec
   `…$AdvancedSearch$dateMiseEnLigneStart/End` (jj/mm/aaaa ; filtrent en réalité la
   date limite de remise des plis) : 10 résultats par page et le compteur
   `resultSearch_nombreElement`.
3. POST, cible `ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop`, ce champ à `20` :
   page 1 avec 20 résultats.
4. Pour chaque page suivante : POST, cible `ctl0$CONTENU_PAGE$resultSearch$DefaultButtonTop`
   et `ctl0$CONTENU_PAGE$resultSearch$numPageTop` = numéro de page.

DCE anonyme (`/index.php?page=Entreprise.EntrepriseDemandeTelechargementDce&id={refCons}
&orgAcronyme={orgCons}`, même formulaire sur Maximilien) :

1. GET du formulaire.
2. POST, cible `ctl0$CONTENU_PAGE$validateButton`, avec
   `ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup` = `…$choixAnonyme` :
   page « Téléchargement complet » avec le lien `EntrepriseDownloadDce$completeDownload`.
3. POST, cible `ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload` : le fichier,
   en `Transfer-Encoding: chunked` (pas de `Content-Length`), `Content-Type:
   application/zip`, `Content-Disposition: attachment; filename="1-DCE.zip";`.

Le téléchargement n'expose aucun identifiant propre au DCE : pas de redirection ni de
paramètre d'URL, et le nom de fichier est celui choisi par l'acheteur (souvent
générique, ex. `1-DCE.zip`). `DceTelecharge.id_dce` vaut donc `None`.
"""

import email.message
from dataclasses import dataclass
from datetime import date
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
from src.tasks.scrap.deao.atexo.parse import (
    ConsultationListe,
    DetailConsultation,
    Lot,
    champs_formulaire,
    nombre_resultats,
    parse_detail,
    parse_lots,
    parse_resultats,
)
from src.tasks.scrap.deao.limiteur import Limiteur

RESULTATS_PAR_PAGE = 20
DATE_DEBUT_RECHERCHE = "01/01/2000"

_RECHERCHE = "ctl0$CONTENU_PAGE$AdvancedSearch$"
_RESULTATS = "ctl0$CONTENU_PAGE$resultSearch$"
_DEMANDE = "ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$"
_VALIDER = "ctl0$CONTENU_PAGE$validateButton"
_TELECHARGEMENT_COMPLET = "ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload"


class RechercheIncomplete(RuntimeError):
    """La recherche n'a pas pu lire toutes les consultations annoncées.

    `consultations` : celles lues avant l'arrêt ; `annoncees` : le compteur du site
    (None s'il n'a pas été lu)."""

    def __init__(
        self,
        message: str,
        consultations: list[ConsultationListe] | None = None,
        annoncees: int | None = None,
    ):
        super().__init__(message)
        self.consultations = consultations or []
        self.annoncees = annoncees


class DceIndisponible(RuntimeError):
    """La plateforme ne propose pas de DCE téléchargeable pour cette consultation."""


@dataclass(frozen=True)
class DceTelecharge:
    chemin: Path
    nom_fichier: str
    content_type: str
    # Aucun identifiant propre au DCE n'est exposé par les plateformes observées.
    id_dce: str | None


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


def _dans_dix_ans(jour: date) -> date:
    try:
        return jour.replace(year=jour.year + 10)
    except ValueError:  # 29 février
        return jour.replace(year=jour.year + 10, day=28)


def _nom_fichier(content_disposition: str | None) -> str | None:
    """Nom de fichier d'un en-tête Content-Disposition (filename ou filename*)."""
    if not content_disposition:
        return None
    message = email.message.Message()
    message["content-disposition"] = content_disposition
    nom = message.get_filename()
    return Path(nom).name if nom else None


class ClientAtexo:
    """Accès HTTP à une plateforme Atexo MPE, sur une seule session (cookies PRADO).

    Les postbacks PRADO sont séquentiels : ne pas partager un client entre plusieurs fils.
    """

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

    def _postback(self, url: str, html: str, cible: str, **champs: str) -> str:
        """Postback PRADO : champs de la page précédente, cible et valeurs imposées."""
        formulaire = champs_formulaire(html)
        formulaire.update(champs)
        formulaire["PRADO_POSTBACK_TARGET"] = cible
        return self._requete("POST", url, data=formulaire).text

    def rechercher(self) -> list[ConsultationListe]:
        """Toutes les consultations en cours, dédupliquées sur refCons."""
        consultations: dict[str, ConsultationListe] = {}
        annoncees: list[int] = []
        try:
            return self._rechercher(consultations, annoncees)
        except (httpx.HTTPError, ValueError) as exc:
            raise RechercheIncomplete(
                f"Recherche interrompue sur {self.url_base} : {exc}",
                list(consultations.values()),
                annoncees[0] if annoncees else None,
            ) from exc

    def _rechercher(
        self, consultations: dict[str, ConsultationListe], annoncees: list[int]
    ) -> list[ConsultationListe]:
        """Remplit `consultations` au fil des pages et `annoncees` du compteur du site."""
        url = f"{self.url_base}/?page=Entreprise.EntrepriseAdvancedSearch&searchAnnCons"
        html = self._requete("GET", url).text
        html = self._postback(
            url,
            html,
            f"{_RECHERCHE}lancerRecherche",
            **{
                f"{_RECHERCHE}dateMiseEnLigneStart": DATE_DEBUT_RECHERCHE,
                f"{_RECHERCHE}dateMiseEnLigneEnd": (
                    f"{_dans_dix_ans(date.today()):%d/%m/%Y}"
                ),
            },
        )
        total = nombre_resultats(html)
        annoncees.append(total)
        if total == 0:
            return []

        html = self._postback(
            url,
            html,
            f"{_RESULTATS}listePageSizeTop",
            **{f"{_RESULTATS}listePageSizeTop": str(RESULTATS_PAR_PAGE)},
        )
        page = 1
        while True:
            lues = parse_resultats(html)
            for c in lues:
                consultations.setdefault(c.ref_cons, c)
            page += 1
            if not lues or page > ceil(total / RESULTATS_PAR_PAGE):
                break
            html = self._postback(
                url,
                html,
                f"{_RESULTATS}DefaultButtonTop",
                **{f"{_RESULTATS}numPageTop": str(page)},
            )

        if len(consultations) != total:
            raise RechercheIncomplete(
                f"{len(consultations)} consultations distinctes lues sur {total} "
                f"annoncées ({self.url_base})",
                list(consultations.values()),
                total,
            )
        return list(consultations.values())

    def close(self) -> None:
        self.http.close()

    def detail(self, ref_cons: str, org_cons: str) -> DetailConsultation:
        url = (
            f"{self.url_base}/entreprise/consultation/{ref_cons}?orgAcronyme={org_cons}"
        )
        return parse_detail(self._requete("GET", url).text)

    def lots(self, ref_cons: str, org_cons: str) -> list[Lot]:
        url = (
            f"{self.url_base}/index.php?page=Entreprise.PopUpDetailLots"
            f"&orgAccronyme={org_cons}&id={ref_cons}&lang="
        )
        return parse_lots(self._requete("GET", url).text)

    def telecharger_dce(
        self, ref_cons: str, org_cons: str, dest_dir: Path
    ) -> DceTelecharge:
        """Télécharge le DCE complet en anonyme, en streaming vers `dest_dir`."""
        url = (
            f"{self.url_base}/index.php?page=Entreprise.EntrepriseDemandeTelechargementDce"
            f"&id={ref_cons}&orgAcronyme={org_cons}"
        )
        html = self._requete("GET", url).text
        html = self._postback(
            url, html, _VALIDER, **{f"{_DEMANDE}RadioGroup": f"{_DEMANDE}choixAnonyme"}
        )
        if _TELECHARGEMENT_COMPLET not in html:
            raise DceIndisponible(
                f"Pas de téléchargement complet proposé pour {ref_cons} ({self.url_base})"
            )
        formulaire = champs_formulaire(html)
        formulaire["PRADO_POSTBACK_TARGET"] = _TELECHARGEMENT_COMPLET
        return self._telecharger_flux(url, formulaire, ref_cons, Path(dest_dir))

    @_retry
    def _telecharger_flux(
        self, url: str, formulaire: dict[str, str], ref_cons: str, dest_dir: Path
    ) -> DceTelecharge:
        self.limiteur.attendre()
        with self.http.stream("POST", url, data=formulaire) as reponse:
            reponse.raise_for_status()
            content_type = reponse.headers.get("content-type", "")
            if content_type.startswith("text/html"):
                raise DceIndisponible(
                    f"Page HTML au lieu du DCE pour {ref_cons} ({self.url_base})"
                )
            nom = _nom_fichier(reponse.headers.get("content-disposition"))
            nom = nom or f"{ref_cons}.zip"
            chemin = dest_dir / f"{ref_cons}{Path(nom).suffix}"
            with open(chemin, "wb") as fichier:
                for morceau in reponse.iter_bytes():
                    fichier.write(morceau)
        return DceTelecharge(
            chemin=chemin, nom_fichier=nom, content_type=content_type, id_dce=None
        )

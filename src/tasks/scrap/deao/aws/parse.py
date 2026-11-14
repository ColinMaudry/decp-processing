"""Parsing HTML de www.marches-publics.info (AWS) : liste de résultats et page d'avis.

Fonctions pures, sans accès réseau.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from html import unescape

from lxml import html as lxml_html

from src.tasks.scrap.deao.plateformes import FUSEAU

NATURES = {"t": "Travaux", "s": "Services", "f": "Fournitures"}
# Panneaux des onglets de la liste : id="1" travaux, "2" services, "3" fournitures
_ONGLET_PANNEAU = {"t": "1", "s": "2", "f": "3"}
_PANNEAU_NATURE = {"1": "Travaux", "2": "Services", "3": "Fournitures"}

_DATE_HEURE = r"\d{1,2}/\d{1,2}/\d{2,4}\s*à\s*\d{1,2}h\d{2}"


@dataclass(frozen=True)
class AvisListe:
    idm: str
    numero_avis: str
    chemin_avis: str
    acheteur_nom: str
    code_postal: str | None
    reference: str | None
    objet: str
    nombre_lots: int
    date_publication: date
    # None si le bloc n'affiche aucune date limite exploitable
    date_limite: datetime | None
    nature: str
    considerations_sociales: bool
    considerations_environnementales: bool


@dataclass(frozen=True)
class LotAvis:
    numero: str
    libelle: str
    description: str | None
    lieu: str | None
    code_postal_lieu: str | None
    montant_estime: float | None
    code_cpv: str | None


@dataclass(frozen=True)
class Avis:
    siret: str | None
    acheteur_nom: str
    adresse_voie: str | None
    adresse_cp: str | None
    adresse_ville: str | None
    objet: str
    reference: str | None
    type_marche: str | None
    mode: str | None
    technique: str | None
    nuts: str | None
    duree_mois: int | None
    description: str | None
    cpv_principal: str | None
    lots: list[LotAvis]
    date_limite: datetime | None
    date_envoi: date | None
    marche_reserve: bool | None
    variantes: str | None
    groupement: bool | None
    # True/False seulement si la page d'avis le dit explicitement, sinon None
    considerations_sociales: bool | None
    considerations_environnementales: bool | None


def _texte(noeud) -> str:
    """Texte d'un nœud lxml, espaces normalisés."""
    if noeud is None:
        return ""
    return " ".join(noeud.text_content().split())


def _norm(texte: str) -> str:
    return " ".join(texte.replace("\xa0", " ").split())


def _arbre(html: str):
    return lxml_html.fromstring(html)


def _lignes(noeud) -> list[str]:
    """Lignes de texte d'un nœud, les `<br>` étant des séparateurs."""
    copie = lxml_html.fromstring(lxml_html.tostring(noeud))
    for br in copie.iter("br"):
        br.tail = "\n" + (br.tail or "")
    lignes = (_norm(ligne) for ligne in copie.text_content().split("\n"))
    return [ligne for ligne in lignes if ligne]


def date_courte(texte: str) -> date:
    """« 10/11/26 » -> date(2026, 11, 10) (années à deux chiffres lues comme 20xx)."""
    m = re.fullmatch(r"\s*(\d{1,2})/(\d{1,2})/(\d{2}|\d{4})\s*", texte)
    if not m:
        raise ValueError(f"Date illisible : {texte!r}")
    annee = int(m[3])
    if annee < 100:
        annee += 2000
    return date(annee, int(m[2]), int(m[1]))


def date_heure_courte(texte: str) -> datetime | None:
    """« 02/12/26 à 12h00 » -> datetime Europe/Paris ; illisible -> None."""
    m = re.fullmatch(
        r"\s*(\d{1,2}/\d{1,2}/(?:\d{2}|\d{4}))\s*à\s*(\d{1,2})h(\d{2})\s*",
        texte or "",
    )
    if not m:
        return None
    try:
        jour = date_courte(m[1])
        return datetime(
            jour.year, jour.month, jour.day, int(m[2]), int(m[3]), tzinfo=FUSEAU
        )
    except ValueError:
        return None


def trop_de_resultats(html: str) -> int | None:
    """« Votre recherche trouve 4093 résultats » -> 4093 ; sinon None."""
    m = re.search(r"Votre recherche trouve\s+([\d\s\xa0]+?)\s*résultats?", html)
    return int(re.sub(r"\D", "", m[1])) if m else None


def aucun_resultat(html: str) -> bool:
    """« Aucun résultat trouvé. Veuillez modifier votre recherche » (recherche à 0)."""
    texte = _norm(unescape(re.sub(r"<[^>]+>", " ", html)))
    return re.search(r"Aucun\s+résultat\s+trouvé", texte, re.IGNORECASE) is not None


def badges(html: str) -> dict[str, int]:
    """Compte de résultats par onglet : clés « t », « s », « f »."""
    arbre = _arbre(html)
    resultat: dict[str, int] = {}
    for cle, nom in (("t", "travaux"), ("s", "services"), ("f", "fournitures")):
        noeuds = arbre.xpath(f'//*[@id="badge_{nom}"]')
        chiffres = re.sub(r"\D", "", _texte(noeuds[0])) if noeuds else ""
        if chiffres:
            resultat[cle] = int(chiffres)
    return resultat


def departements(html: str) -> list[str]:
    """Valeurs « département seul » du `<select name="IDR">` (sans X ni regroupements)."""
    arbre = _arbre(html)
    valeurs: list[str] = []
    for option in arbre.xpath('//select[@name="IDR"]/option'):
        valeur = (option.get("value") or "").strip()
        if not valeur or valeur == "X" or "," in valeur or valeur in valeurs:
            continue
        valeurs.append(valeur)
    return valeurs


def _idm(bloc) -> str | None:
    for href in bloc.xpath(".//a/@href"):
        # IDM de www.marches-publics.info seulement : dl.aws-achat.info a sa numérotation
        if href.startswith("http") and "://www.marches-publics.info/" not in href:
            continue
        m = re.search(r"[?&]IDM=(\d+)", href)
        if m:
            return m[1]
    return None


def _bloc_vers_avis(bloc, nature: str) -> AvisListe | None:
    idm = _idm(bloc)
    if idm is None:
        return None
    liens = bloc.xpath('.//a[contains(@href, "/Annonces/MPI-pub-")]/@href')
    if not liens:
        return None
    chemin = re.sub(r"^https?://[^/]+", "", liens[0])
    m = re.search(r"MPI-pub-(\d+)\.htm", chemin)
    if not m:
        return None
    h2 = bloc.xpath('.//h2[contains(@class, "h2-avis")]')
    h2_texte = _texte(h2[0]) if h2 else ""
    m_cp = re.search(r"\((\d{5})\)\s*$", h2_texte)
    code_postal = m_cp[1] if m_cp else None
    nom = re.sub(r"\s*\(\d{5}\)\s*$", "", h2_texte).strip()

    boite = bloc.xpath('.//*[@id="titre_box"]')
    reference = None
    objet = ""
    nombre_lots = 0
    if boite:
        ref = boite[0].xpath('.//*[contains(@class, "ref-acheteur")]')
        if ref:
            m_ref = re.search(r"\[réf\.\s*(.*?)\]", _texte(ref[0]))
            reference = m_ref[1].strip() or None if m_ref else None
        objet = _norm(" ".join(boite[0].xpath("./text()")))
        m_lots = re.search(r"alloti\s*:\s*(\d+)\s*lots?", _texte(boite[0]))
        nombre_lots = int(m_lots[1]) if m_lots else 0

    texte = _texte(bloc)
    m_pub = re.search(r"Publié le\s*(\d{1,2}/\d{1,2}/\d{2,4})", texte)
    if not m_pub:
        return None
    try:
        date_publication = date_courte(m_pub[1])
    except ValueError:
        return None
    m_lim = re.search(r"Date limite\s*:\s*le\s*(" + _DATE_HEURE + ")", texte)
    images = " ".join(bloc.xpath(".//img/@src"))
    return AvisListe(
        idm=idm,
        numero_avis=m[1],
        chemin_avis=chemin,
        acheteur_nom=nom,
        code_postal=code_postal,
        reference=reference,
        objet=objet,
        nombre_lots=nombre_lots,
        date_publication=date_publication,
        date_limite=date_heure_courte(m_lim[1]) if m_lim else None,
        nature=nature,
        considerations_sociales="ico-critere-social" in images,
        considerations_environnementales="ico-critere-environnement" in images,
    )


def _blocs(arbre, onglet: str | None):
    """Blocs de résultats `(bloc, nature)` de l'onglet demandé (None = tous)."""
    for bloc in arbre.xpath('//div[@id="entity"]'):
        panneaux = bloc.xpath('ancestor::div[contains(@class, "tab-pane")][1]/@id')
        panneau = panneaux[0] if panneaux else None
        if onglet is not None and panneau != _ONGLET_PANNEAU[onglet]:
            continue
        nature = _PANNEAU_NATURE.get(panneau or "")
        if nature is not None:
            yield bloc, nature


def parse_liste(html: str, onglet: str | None = None) -> list[AvisListe]:
    """Avis d'une page de résultats ; `onglet` « t », « s » ou « f » (None = tous).

    Un bloc sans IDM (aucun lien DCE, RC, question ou dépôt) ou sans date de
    publication lisible est ignoré : c'est à l'appelant de compter l'écart avec
    le badge (voir `numeros_avis`).
    """
    resultat: list[AvisListe] = []
    for bloc, nature in _blocs(_arbre(html), onglet):
        avis = _bloc_vers_avis(bloc, nature)
        if avis is not None:
            resultat.append(avis)
    return resultat


def numeros_avis(html: str, onglet: str | None = None) -> list[str]:
    """Clé d'avis `{PREFIXE}-{n°}` (ex. `MPI-2026295098`, `DL-2026303005`) par bloc.

    Compte tous les blocs ayant un lien `/Annonces/{PREFIXE}-pub-{n°}.htm`, avec ou
    sans IDM, y compris ceux d'une autre plateforme AWS (`DL`) : c'est la base de la
    comparaison avec le badge de l'onglet et avec le total national. Le préfixe évite
    toute collision entre numérotations.
    """
    numeros: list[str] = []
    for bloc, _ in _blocs(_arbre(html), onglet):
        for lien in bloc.xpath('.//a[contains(@href, "/Annonces/")]/@href'):
            m = re.search(r"/Annonces/([A-Za-z0-9]+)-pub-(\d+)\.htm", lien)
            if m:
                numeros.append(f"{m[1]}-{m[2]}")
                break
    return numeros


def _oui_non(texte: str, motif: str) -> bool | None:
    m = re.search(motif + r"\s*:\s*(Oui|Non)\b", texte)
    if not m:
        return None
    return m[1] == "Oui"


def _variantes(texte: str) -> str | None:
    """exigées / autorisées / interdites, ou None si l'avis ne tranche pas."""
    m = re.search(
        r"variantes?\s+(?:sont|est)\s+(exigées|autorisées|acceptées|refusées|interdites)"
        r"(?:\s*:\s*(Oui|Non)\b)?",
        texte,
    )
    if m:
        mot, reponse = m[1], m[2]
        if mot in ("refusées", "interdites"):
            return "interdites"
        if mot == "exigées":
            return "exigées" if reponse == "Oui" else None
        # autorisées / acceptées
        return "interdites" if reponse == "Non" else "autorisées"
    # Variantes déclarées lot par lot : retenues si tous les lots disent pareil
    reponses = set(re.findall(r"Acceptation des variantes\s*:\s*(Oui|Non)\b", texte))
    if reponses == {"Oui"}:
        return "autorisées"
    if reponses == {"Non"}:
        return "interdites"
    return None


def _cellules_libelle(arbre) -> dict[str, object]:
    """Lignes « libellé / valeur » du tableau principal, indexées par libellé."""
    lignes: dict[str, object] = {}
    for tr in arbre.xpath("//tr"):
        tds = tr.xpath("./td")
        if len(tds) != 2:
            continue
        libelle = _texte(tds[0])
        if libelle and libelle not in lignes and len(libelle) < 60:
            lignes[libelle] = tds[1]
    return lignes


def _entete_acheteur(
    arbre,
) -> tuple[str, str | None, str | None, str | None, str | None]:
    """(nom, siret, voie, code postal, ville) du bloc acheteur."""
    for td in arbre.xpath('//td[@valign="top"][b]'):
        if "SIRET" not in td.xpath("string(.)"):
            continue
        # `td/text()` seulement : le tableau imbriqué (liens) est exclu
        gras = td.xpath("./b[1]")
        nom = _texte(gras[0]) if gras else ""
        lignes = [_norm(t) for t in td.xpath("./text()") if _norm(t)]
        siret = None
        for ligne in lignes:
            m = re.search(r"SIRET\s*(\d{14})", ligne)
            if m:
                siret = m[1]
        cp = ville = voie = None
        for i, ligne in enumerate(lignes):
            m = re.fullmatch(r"(\d{5})\s+(.+)", ligne)
            if m:
                cp, ville = m[1], m[2]
                # la ligne qui précède est la voie, sauf si c'est le représentant
                if i > 0 and not re.search(r"\s-\s", lignes[i - 1]):
                    voie = lignes[i - 1]
                break
        return nom, siret, voie, cp, ville
    return "", None, None, None, None


def _lots(arbre) -> list[LotAvis]:
    lots: list[LotAvis] = []
    for tr in arbre.xpath("//tr"):
        tds = tr.xpath("./td")
        if len(tds) != 4:
            continue
        m = re.fullmatch(r"N°\s*(\S+)", _texte(tds[0]))
        if not m:
            continue
        lignes = _lignes(tds[1])
        libelle = lignes[0] if lignes else ""
        description = None
        lieu = None
        morceaux: list[str] | None = None
        for ligne in lignes[1:]:
            if ligne.startswith("Description :"):
                morceaux = [ligne.removeprefix("Description :").strip()]
            elif ligne.startswith("Lieu d'exécution"):
                morceaux = None
                lieu = ligne.split(":", 1)[1].strip() or None
            elif morceaux is not None:
                if re.match(r"(Durée|Acceptation|Options|Reconductions|Fonds)", ligne):
                    morceaux = None
                else:
                    morceaux.append(ligne)
            if morceaux is not None:
                description = " ".join(p for p in morceaux if p) or None
        m_cp = re.search(r"\b(\d{5})\b", lieu or "")
        m_montant = re.search(
            r"\d[\d\s]*(?:,\d+)?", tds[2].text_content().replace("\xa0", " ")
        )
        montant = None
        if m_montant:
            try:
                montant = float(m_montant[0].replace(" ", "").replace(",", "."))
            except ValueError:
                montant = None
        m_cpv = re.search(r"\b(\d{8})\b", tds[3].text_content())
        lots.append(
            LotAvis(
                numero=m[1],
                libelle=libelle,
                description=description,
                lieu=lieu,
                code_postal_lieu=m_cp[1] if m_cp else None,
                montant_estime=montant,
                code_cpv=m_cpv[1] if m_cpv else None,
            )
        )
    return lots


def parse_avis(html: str) -> Avis:
    """Page d'avis `/Annonces/MPI-pub-{n°}.htm`.

    Une ligne absente donne None ; sans objet ni acheteur, ValueError.
    """
    arbre = _arbre(html)
    for noeud in arbre.xpath("//script|//style"):
        noeud.drop_tree()
    texte = _norm(arbre.text_content())
    lignes = _cellules_libelle(arbre)

    def valeur(*libelles: str) -> str | None:
        for libelle in libelles:
            if libelle in lignes:
                v = _texte(lignes[libelle])
                return v or None
        return None

    nom, siret, voie, cp, ville = _entete_acheteur(arbre)
    objet = valeur("Objet")
    if not objet and not nom:
        raise ValueError("Page d'avis sans objet ni acheteur")

    nuts = None
    lieu = valeur("Lieu d'exécution principal", "Lieu principal d'exécution")
    if lieu:
        m = re.match(r"([A-Z]{2}[A-Z0-9]{1,3})\b", lieu)
        nuts = m[1] if m else None

    duree = None
    m_duree = re.search(r"(\d+)\s*mois", valeur("Durée") or "") or re.search(
        r"Durée\s*:\s*(\d+)\s*mois", texte
    )
    if m_duree:
        duree = int(m_duree[1])

    cpv = None
    if "Code CPV principal" in lignes:
        m_cpv = re.search(r"\b(\d{8})\b", _texte(lignes["Code CPV principal"]))
        cpv = m_cpv[1] if m_cpv else None

    m_limite = re.search(r"Remise des offres le\s*(" + _DATE_HEURE + ")", texte)
    m_envoi = re.search(r"Envoi le\s*(\d{1,2}/\d{1,2}/\d{2,4})", texte)
    date_envoi = None
    if m_envoi:
        try:
            date_envoi = date_courte(m_envoi[1])
        except ValueError:
            date_envoi = None

    return Avis(
        siret=siret,
        acheteur_nom=nom,
        adresse_voie=voie,
        adresse_cp=cp,
        adresse_ville=ville,
        objet=objet or "",
        reference=valeur("Référence"),
        type_marche=valeur("Type de marché"),
        mode=valeur("Mode"),
        technique=valeur("Technique d'achat"),
        nuts=nuts,
        duree_mois=duree,
        description=valeur("Description"),
        cpv_principal=cpv,
        lots=_lots(arbre),
        date_limite=date_heure_courte(m_limite[1]) if m_limite else None,
        date_envoi=date_envoi,
        marche_reserve=_oui_non(texte, r"Marché réservé"),
        variantes=_variantes(texte),
        groupement=_oui_non(texte, r"Groupement de commandes"),
        considerations_sociales=_oui_non(texte, r"Considérations? sociales?"),
        considerations_environnementales=_oui_non(
            texte, r"Considérations? environnementales?"
        ),
    )

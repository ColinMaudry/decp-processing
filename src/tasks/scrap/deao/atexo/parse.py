"""Parsing HTML des plateformes Atexo MPE (PRADO) : pages de résultats, détail et lots.

Fonctions pures, sans accès réseau.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from lxml import html as lxml_html

FUSEAU = ZoneInfo("Europe/Paris")

MOIS = {
    "jan": 1,
    "janv": 1,
    "janvier": 1,
    "fev": 2,
    "févr": 2,
    "fevr": 2,
    "février": 2,
    "mars": 3,
    "avr": 4,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "juill": 7,
    "juillet": 7,
    "août": 8,
    "aout": 8,
    "sep": 9,
    "sept": 9,
    "septembre": 9,
    "oct": 10,
    "octobre": 10,
    "nov": 11,
    "novembre": 11,
    "déc": 12,
    "dec": 12,
    "décembre": 12,
}


@dataclass(frozen=True)
class ConsultationListe:
    ref_cons: str
    org_cons: str
    procedure_code: str
    categorie: str
    date_mise_en_ligne: date
    reference: str
    intitule: str
    objet: str
    organisme: str
    code_postal: str | None
    ville: str | None
    nombre_lots: int
    lieux_execution: list[str]
    # None si le bloc de la liste n'affiche aucune date limite exploitable
    date_limite: datetime | None


@dataclass(frozen=True)
class DetailConsultation:
    code_cpv: str | None
    entite_publique: str | None
    service: str | None
    taille_dce: str | None


@dataclass(frozen=True)
class Lot:
    numero: str
    intitule: str
    categorie: str | None
    description: str | None
    code_cpv: str | None


def _texte(noeud) -> str:
    """Texte d'un nœud lxml, espaces normalisés."""
    if noeud is None:
        return ""
    return " ".join(noeud.text_content().split())


def _arbre(html: str):
    return lxml_html.fromstring(html)


def _mois(texte: str) -> int:
    cle = texte.strip().rstrip(".").lower()
    if cle not in MOIS:
        raise ValueError(f"Mois inconnu dans la date : {texte!r}")
    return MOIS[cle]


def date_fr(texte: str) -> date:
    """« 19 Oct. 2026 » -> date(2026, 10, 19)."""
    m = re.fullmatch(r"\s*(\d{1,2})\s+(\S+?)\s+(\d{4})\s*", texte)
    if not m:
        raise ValueError(f"Date illisible : {texte!r}")
    return date(int(m[3]), _mois(m[2]), int(m[1]))


def date_heure_fr(texte: str) -> datetime:
    """« 13 Nov. 2026 16:45 » -> datetime avec fuseau Europe/Paris."""
    m = re.fullmatch(r"\s*(.+?)\s+(\d{1,2}):(\d{2})\s*", texte)
    if not m:
        raise ValueError(f"Date et heure illisibles : {texte!r}")
    jour = date_fr(m[1])
    return datetime(
        jour.year, jour.month, jour.day, int(m[2]), int(m[3]), tzinfo=FUSEAU
    )


def champs_formulaire(html: str) -> dict[str, str]:
    """Champs nommés du formulaire (cases et radios cochées seulement)."""
    arbre = _arbre(html)
    champs: dict[str, str] = {}
    for el in arbre.iter("input", "select", "textarea"):
        nom = el.get("name")
        if not nom:
            continue
        if el.tag == "input":
            type_ = (el.get("type") or "text").lower()
            if type_ in ("submit", "button", "image", "reset", "file"):
                continue
            if type_ in ("checkbox", "radio") and el.get("checked") is None:
                continue
            champs[nom] = el.get("value") or (
                "on" if type_ in ("checkbox", "radio") else ""
            )
        elif el.tag == "select":
            options = el.findall(".//option")
            choisie = next((o for o in options if o.get("selected") is not None), None)
            if choisie is None and options:
                choisie = options[0]
            if choisie is not None:
                champs[nom] = choisie.get("value", _texte(choisie))
        else:
            champs[nom] = el.text_content()
    return champs


def nombre_resultats(html: str) -> int:
    """Compteur « Nombre de résultats : N » ; 0 si « Aucun résultat »."""
    arbre = _arbre(html)
    for el in arbre.xpath('//*[contains(@id, "resultSearch_nombreElement")]'):
        m = re.search(r"\d+", _texte(el))
        if m:
            return int(m[0])
    texte = _texte(arbre)
    if "Aucun résultat" in texte:
        return 0
    raise ValueError(f"Compteur de résultats introuvable, page : {texte[:200]!r}")


def _date_bloc(noeud) -> str:
    """Texte « jour mois année » d'un bloc .date."""
    jour = _texte(_premier(noeud, './/div[normalize-space(@class)="day"]'))
    mois = _texte(_premier(noeud, './/div[normalize-space(@class)="month"]'))
    annee = _texte(_premier(noeud, './/div[normalize-space(@class)="year"]'))
    return f"{jour} {mois} {annee}"


def _premier(bloc, xpath: str):
    res = bloc.xpath(xpath)
    return res[0] if res else None


def _parse_bloc(bloc) -> ConsultationListe:
    ref = _premier(bloc, './/input[contains(@name, "$refCons")]')
    org = _premier(bloc, './/input[contains(@name, "$orgCons")]')
    if ref is None or org is None:
        raise ValueError("Bloc de consultation sans refCons / orgCons")

    organisme = _texte(
        _premier(
            bloc, './/div[contains(@id, "panelBlocDenomination")]//span[@class="small"]'
        )
    )
    code_postal = ville = None
    m = re.fullmatch(r"(.*?)\s*\((\d[\dAB]*)\s*-\s*(.*?)\s*\)", organisme)
    if m:
        organisme, code_postal, ville = m[1], m[2], m[3] or None

    nb_lots = 0
    lots = _premier(bloc, './/div[@class="lots"]')
    if lots is not None:
        m = re.search(r"\d+", _texte(lots))
        nb_lots = int(m[0]) if m else 0

    lieux: list[str] = []
    zone_lieux = _premier(bloc, './/div[contains(@class, "lieux-exe")]')
    if zone_lieux is not None:
        for code in re.findall(r"\(([0-9][0-9AB]\d?)\)", _texte(zone_lieux)):
            if code not in lieux:
                lieux.append(code)

    date_limite = None
    cloture = _premier(bloc, './/div[contains(@class, "cloture-line")]')
    if cloture is not None:
        jour = _premier(cloture, './/div[contains(@class, "date")]')
        heure = _texte(_premier(cloture, './/div[contains(@class, "time")]'))
        if jour is not None and re.fullmatch(r"\d{1,2}:\d{2}", heure):
            date_limite = date_heure_fr(f"{_date_bloc(jour)} {heure}")

    ligne_objet = _premier(bloc, './/div[contains(@class, "objet-line")]')
    reference = _texte(ligne_objet.find('.//div[@class="small pull-left"]'))
    intitule = _texte(_premier(ligne_objet, './/div[contains(@class, "truncate")]'))
    objet = _texte(
        _premier(bloc, './/div[contains(@id, "panelBlocObjet")]//span[@class="small"]')
    )

    return ConsultationListe(
        ref_cons=ref.get("value"),
        org_cons=org.get("value"),
        procedure_code=_texte(_premier(bloc, './/div[@class="cons_procedure"]//span')),
        categorie=_texte(
            _premier(bloc, './/div[contains(@class, "cons_categorie")]//span')
        ),
        date_mise_en_ligne=date_fr(
            _date_bloc(
                _premier(
                    bloc,
                    './/div[contains(@class, "cons_ref")]//div[contains(@class, "date")]',
                )
            )
        ),
        reference=reference,
        intitule=intitule,
        objet=objet,
        organisme=organisme,
        code_postal=code_postal,
        ville=ville,
        nombre_lots=nb_lots,
        lieux_execution=lieux,
        date_limite=date_limite,
    )


def parse_resultats(html: str) -> list[ConsultationListe]:
    """Consultations d'une page de résultats (liste vide si aucun résultat).

    Un bloc illisible lève ValueError : la recherche est alors incomplète (rien n'est
    annulé) au lieu d'échouer sur une erreur imprévue."""
    arbre = _arbre(html)
    consultations = []
    for i, bloc in enumerate(
        arbre.xpath('//div[contains(@class, "item_consultation")]'), start=1
    ):
        try:
            consultations.append(_parse_bloc(bloc))
        except Exception as e:
            raise ValueError(f"Bloc de consultation n° {i} illisible : {e!r}") from e
    return consultations


def _valeur_libelle(arbre, libelle: str):
    """Nœud valeur du <li> dont le <label> commence par `libelle`."""
    return _premier(
        arbre,
        f'//li[label[starts-with(normalize-space(.), "{libelle}")]]/div',
    )


def parse_detail(html: str) -> DetailConsultation:
    """Page de détail d'une consultation (CPV principal, entité, service, taille du DCE)."""
    arbre = _arbre(html)

    code_cpv = None
    cpv = _valeur_libelle(arbre, "Code CPV")
    if cpv is not None:
        m = re.search(r"\d{8}", _texte(cpv))
        code_cpv = m[0] if m else None

    entite = _texte(_valeur_libelle(arbre, "Entité publique")) or None
    service = _texte(_valeur_libelle(arbre, "Service")) or None

    taille = None
    lien = _premier(arbre, '//a[@id="linkDownloadDce"]')
    if lien is not None:
        m = re.search(r"Dossier de consultation\s*-\s*(.+)", _texte(lien))
        taille = m[1].strip() if m else None

    return DetailConsultation(code_cpv, entite, service, taille)


def parse_lots(html: str) -> list[Lot]:
    """Lots de la popup « Descriptif des lots »."""
    arbre = _arbre(html)
    lots = []
    for panneau in arbre.xpath(
        '//div[contains(@class, "panel-default")][.//div[contains(@class, "panel-heading")]]'
    ):
        entete = _premier(panneau, './/div[contains(@class, "panel-heading")]')
        m = re.match(r"Lot\s+(\S+?)\s*:\s*(.*)", _texte(entete))
        if not m:
            continue
        champs = {}
        for groupe in panneau.xpath('.//div[contains(@class, "form-group")]'):
            label = _texte(groupe.find("label")).rstrip(" :")
            valeur = _premier(groupe, "div")
            if label and valeur is not None:
                champs[label] = _texte(valeur) or None
        cpv = re.search(r"(\d{8})\s*\(Code principal\)", _texte(panneau))
        lots.append(
            Lot(
                numero=m[1],
                intitule=m[2],
                categorie=champs.get("Catégorie"),
                description=champs.get("Description"),
                code_cpv=cpv[1] if cpv else None,
            )
        )
    return lots

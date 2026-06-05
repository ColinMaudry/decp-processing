"""Aplatissement générique des annonces BOAMP/eForms (issue #174).

Aucune I/O : fonctions pures sur des éléments lxml.
"""

from src.config import (
    BOAMP_AVIS_URL_TEMPLATE,
    BOAMP_INCLUDED_NATURES,
    BOAMP_MISE_EN_CONCURRENCE_NATURES,
)


def localname(tag) -> str:
    """Nom local d'une balise lxml (sans préfixe de namespace)."""
    if not isinstance(tag, str):  # commentaires, PI…
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def flatten(
    elem,
    start_path: str = "",
    skip_localnames: frozenset | set = frozenset(),
) -> dict[str, list[str]]:
    """Aplatit un sous-arbre en {chemin_feuille: [valeurs...]}.

    - chemin = noms locaux joints par "_", préfixé par ``start_path`` ;
    - seules les feuilles (sans enfant élément) avec texte non vide sont gardées ;
    - les feuilles répétées accumulent plusieurs valeurs ;
    - les sous-arbres dont le nom local est dans ``skip_localnames`` sont ignorés ;
    - les attributs ne sont pas capturés en v1.
    """
    acc: dict[str, list[str]] = {}

    def walk(e, path: str):
        children = [c for c in e if isinstance(c.tag, str)]
        if not children:
            text = (e.text or "").strip()
            if text:
                acc.setdefault(path, []).append(text)
            return
        for child in children:
            name = localname(child.tag)
            if name in skip_localnames:
                continue
            child_path = f"{path}_{name}" if path else name
            walk(child, child_path)

    # If start_path is empty, use the root element's localname
    if not start_path:
        start_path = localname(elem.tag)

    walk(elem, start_path)
    return acc


def _find(elem, name: str):
    """Premier descendant (ou self) dont le nom local == name, sinon None."""
    if localname(elem.tag) == name:
        return elem
    for e in elem.iter():
        if localname(e.tag) == name:
            return e
    return None


def _first_child_localname(elem) -> str | None:
    """Nom local du premier enfant élément, sinon None."""
    for c in elem:
        if isinstance(c.tag, str):
            return localname(c.tag)
    return None


def detect_format(root) -> str:
    """'eforms' si un élément EFORMS est présent, sinon 'boamp' (legacy v240)."""
    return "eforms" if _find(root, "EFORMS") is not None else "boamp"


def parse_type_avis(root):
    """(nature, sous_nature, statut, famille) depuis GESTION/REFERENCE/TYPE_AVIS.

    Chaque valeur est le nom local de l'enfant (ex. NATURE -> 'APPEL_OFFRE',
    sous-nature = enfant de cet enfant -> 'MISE_EN_CONCURRENCE'), ou None.
    """
    type_avis = _find(root, "TYPE_AVIS")
    if type_avis is None:
        return None, None, None, None
    nature_el = _find(type_avis, "NATURE")
    nature = _first_child_localname(nature_el) if nature_el is not None else None
    sub = None
    if nature_el is not None:
        nature_child = next((c for c in nature_el if isinstance(c.tag, str)), None)
        if nature_child is not None:
            sub = _first_child_localname(nature_child)
    famille_el = _find(type_avis, "FAMILLE")
    famille = _first_child_localname(famille_el) if famille_el is not None else None
    statut_el = _find(type_avis, "STATUT")
    statut = _first_child_localname(statut_el) if statut_el is not None else None
    return nature, sub, statut, famille


def is_included(nature: str | None, sous_nature: str | None) -> bool:
    """Décide si l'avis entre dans le périmètre (liste blanche NATURE)."""
    if nature in BOAMP_INCLUDED_NATURES:
        return True
    if (
        nature in BOAMP_MISE_EN_CONCURRENCE_NATURES
        and sous_nature == "MISE_EN_CONCURRENCE"
    ):
        return True
    return False


def _text(elem) -> str | None:
    """Extrait le texte d'un élément, ou None si vide."""
    if elem is None:
        return None
    t = (elem.text or "").strip()
    return t or None


def find_lots(root, source_format: str) -> list:
    """Liste des éléments-lot selon le format.

    - eforms: ProcurementProjectLot éléments
    - boamp: lot éléments enfants d'un conteneur lots
    """
    if source_format == "eforms":
        return [e for e in root.iter() if localname(e.tag) == "ProcurementProjectLot"]
    lots = []
    for container in root.iter():
        if localname(container.tag) == "lots":
            lots.extend(c for c in container if localname(c.tag) == "lot")
    return lots


def extract_lot_core(lot, source_format: str) -> dict:
    """{lot_id, lot_intitule} pour un élément-lot.

    - eforms: ID[@schemeName='Lot'] et Name sous ProcurementProject
    - boamp: numLot et description directs
    """
    if source_format == "eforms":
        lot_id = None
        for e in lot.iter():
            if localname(e.tag) == "ID" and e.get("schemeName") == "Lot":
                lot_id = _text(e)
                break
        name_el = next((e for e in lot.iter() if localname(e.tag) == "Name"), None)
        return {"lot_id": lot_id, "lot_intitule": _text(name_el)}
    # legacy boamp
    num = next((c for c in lot if localname(c.tag) == "numLot"), None)
    desc = next((c for c in lot if localname(c.tag) == "description"), None)
    return {"lot_id": _text(num), "lot_intitule": _text(desc)}


def _direct_child(elem, name: str):
    """Premier enfant DIRECT dont le nom local == name, sinon None."""
    return next((c for c in elem if localname(c.tag) == name), None)


def _buyer_org_id(root) -> str | None:
    """ID de l'organisation acheteuse = PartyIdentification/ID enfant DIRECT de
    ContractingParty/Party (et non du ServiceProviderParty imbriqué)."""
    cp = _find(root, "ContractingParty")
    if cp is None:
        return None
    party = _direct_child(cp, "Party")
    if party is None:
        return None
    pid = _direct_child(party, "PartyIdentification")
    if pid is None:
        return None
    return _text(_direct_child(pid, "ID"))


def _find_company_by_id(root, org_id: str):
    """efac:Company dont PartyIdentification/ID == org_id."""
    for org in root.iter():
        if localname(org.tag) != "Organization":
            continue
        company = _find(org, "Company")
        if company is None:
            continue
        pid = _find(company, "PartyIdentification")
        if pid is None:
            continue
        idv = next((e for e in pid.iter() if localname(e.tag) == "ID"), None)
        if _text(idv) == org_id:
            return company
    return None


def eforms_acheteur(root) -> dict:
    """{acheteur_nom, acheteur_siret, acheteur_ville} par déréférencement interne."""
    empty = {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}
    org_id = _buyer_org_id(root)
    if org_id is None:
        return empty
    company = _find_company_by_id(root, org_id)
    if company is None:
        return empty
    name_el = _find(_find(company, "PartyName") or company, "Name")
    siret_el = _find(_find(company, "PartyLegalEntity") or company, "CompanyID")
    city_el = _find(_find(company, "PostalAddress") or company, "CityName")
    return {
        "acheteur_nom": _text(name_el),
        "acheteur_siret": _text(siret_el),
        "acheteur_ville": _text(city_el),
    }


LOT_CONTAINER: dict[str, str] = {
    "eforms": "ProcurementProjectLot",
    "boamp": "lots",
}

EXCLUDE_ELEMENTS = frozenset({"HTML", "HTMLSYNTHESE"})


def annonce_to_rows(root, fichier_source: str) -> list[dict]:
    """Transforme une annonce en lignes (une par lot, ou une seule si pas de lot).

    Retourne [] si l'avis est hors périmètre (filtre NATURE).
    """
    source_format = detect_format(root)
    nature, sous_nature, _statut, _famille = parse_type_avis(root)
    if not is_included(nature, sous_nature):
        return []

    core = extract_core(root, source_format, fichier_source)
    lot_container = LOT_CONTAINER[source_format]
    notice_cols = flatten(root, skip_localnames=EXCLUDE_ELEMENTS | {lot_container})
    lots = find_lots(root, source_format)

    if not lots:
        return [{**core, **notice_cols}]

    rows = []
    for lot in lots:
        lot_core = extract_lot_core(lot, source_format)
        lot_cols = flatten(lot, start_path=lot_container)
        rows.append({**core, **lot_core, **notice_cols, **lot_cols})
    return rows


CORE_KEYS = (
    "source_format",
    "id_annonce",
    "nature_avis",
    "statut_avis",
    "famille_avis",
    "type_document",
    "sous_type_eforms",
    "date_publication",
    "date_limite_reponse",
    "acheteur_nom",
    "acheteur_siret",
    "acheteur_ville",
    "objet",
    "url_avis",
    "fichier_source",
    "lot_id",
    "lot_intitule",
)


def _gestion_text(root, name: str) -> str | None:
    gestion = _find(root, "GESTION")
    if gestion is None:
        return None
    return _text(_find(gestion, name))


def _legacy_objet(root):
    donnees = _find(root, "DONNEES")
    return _text(_find(donnees, "objet")) if donnees is not None else None


def _eforms_objet(root):
    """Name du ProcurementProject de niveau annonce (enfant direct du ContractNotice)."""
    cn = _find(root, "ContractNotice")
    if cn is None:
        return None
    pp = _direct_child(cn, "ProcurementProject")
    return _text(_find(pp, "Name")) if pp is not None else None


def _legacy_acheteur(root) -> dict:
    organisme = _find(root, "organisme")
    if organisme is None:
        return {"acheteur_nom": None, "acheteur_siret": None, "acheteur_ville": None}
    return {
        "acheteur_nom": _text(_find(organisme, "acheteurPublic")),
        "acheteur_siret": None,
        "acheteur_ville": _text(_find(organisme, "ville")),
    }


def extract_core(root, source_format: str, fichier_source: str) -> dict:
    """Cœur harmonisé (colonnes scalaires) pour une annonce."""
    nature, _sub, statut, famille = parse_type_avis(root)
    idweb = _gestion_text(root, "IDWEB")
    prefix = "ef_" if source_format == "eforms" else "bo_"
    id_annonce = f"{prefix}{idweb}" if idweb else None

    if source_format == "eforms":
        acheteur = eforms_acheteur(root)
        objet = _eforms_objet(root)
        type_document = _first_child_localname(_find(root, "EFORMS"))
        sous_type_eforms = _text(_find(root, "SubTypeCode"))
    else:
        acheteur = _legacy_acheteur(root)
        objet = _legacy_objet(root)
        type_document = _first_child_localname(_find(root, "DONNEES"))
        sous_type_eforms = None

    return {
        "source_format": source_format,
        "id_annonce": id_annonce,
        "nature_avis": nature,
        "statut_avis": statut,
        "famille_avis": famille,
        "type_document": type_document,
        "sous_type_eforms": sous_type_eforms,
        "date_publication": _gestion_text(root, "DATE_PUBLICATION"),
        "date_limite_reponse": _gestion_text(root, "DATE_LIMITE_REPONSE"),
        "acheteur_nom": acheteur["acheteur_nom"],
        "acheteur_siret": acheteur["acheteur_siret"],
        "acheteur_ville": acheteur["acheteur_ville"],
        "objet": objet,
        "url_avis": BOAMP_AVIS_URL_TEMPLATE.format(idweb=idweb) if idweb else None,
        "fichier_source": fichier_source,
        "lot_id": None,
        "lot_intitule": None,
    }

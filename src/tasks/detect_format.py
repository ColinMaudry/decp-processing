def detect_format_titulaire(titulaire) -> str or None:
    """
    Détecte le format du champ "titulaire" selon sa structure.

    Cette fonction distingue deux formats :
    - Format 2022 : dictionnaire contenant une clé 'titulaire' elle-même dictionnaire.
    - Format 2019 : dictionnaire contenant directement une clé 'id'.

    Parameters
    ----------
    titulaire : dict
        Dictionnaire représentant un titulaire, dont la structure est utilisée pour
        déterminer le format.

    Returns
    -------
    str or None
        Retourne '2022' ou '2019' selon la structure du champ. Retourne `None` si le format n'est pas reconnu.
    """
    # si titulaire est un dict au format {'titulaire': {'typeIdentifiant', 'id'}} alors format 2022
    if isinstance(titulaire, dict) and "titulaire" in titulaire:
        return "2022"
    elif isinstance(titulaire, dict) and "id" in titulaire:
        return "2019"
    else:
        print(f"Format inconnu pour le titulaire : {titulaire}")
        return None


def detect_format_multiple_titulaires(data: list, quorum: float) -> str:
    """
    Détecte le format dominant parmi plusieurs objets contenant des titulaires.

    Cette fonction applique `detect_format_titulaire` à chaque titulaire présent
    dans la liste `data` et retourne le format majoritaire (au-dessus d'un quorum).

    Parameters
    ----------
    data : list of dict
        Liste d'objets (généralement des marchés) contenant potentiellement une
        clé 'titulaires' (liste).
    quorum : float, optional
        Seuil à partir duquel un format est considéré dominant. Doit être entre 0 et 1.
        Par défaut à 0.7 (70%).

    Returns
    -------
    str
        '2019' si plus de `quorum` des formats détectés sont en 2019, sinon '2022'.
        Retourne 'empty' si `data` est vide.

    Raises
    ------
    Exception
        Si aucun format n'a pu être détecté parmi les titulaires présents.
    """
    formats = []
    if len(data) > 0:
        for el in data:
            if "titulaires" in el and isinstance(el["titulaires"], list):
                formats.extend([detect_format_titulaire(t) for t in el["titulaires"]])
        if len(formats) > 0:
            if formats.count("2019") / len(formats) > quorum:
                return "2019"
            else:
                return "2022"
        else:
            # Dans le doute, on tente le format 2022
            return "2022"
    else:
        return "empty"


def detect_format(data, quorum: float) -> str:
    """
    Détecte automatiquement le format global d’un fichier en analysant ses marchés.

    Cette fonction récupère les marchés depuis le champ `data['marches']`, puis applique
    `detect_format_multiple_titulaires` pour déterminer le format dominant parmi les titulaires.

    Parameters
    ----------
    data : dict
        Données chargées (ex : JSON complet), contenant éventuellement une clé 'marches'.
    quorum : float, optional
        Seuil de majorité pour déterminer le format dominant. Par défaut `QUORUM`.

    Returns
    -------
    str
        '2019', '2022' ou 'empty' selon la structure des données.

    Notes
    -----
    Cette fonction supporte plusieurs structures possibles de `data['marches']` :
    - Une liste de marchés.
    - Un dictionnaire contenant une clé 'marche' (structure imbriquée).
    """
    _data = []
    if "marches" in data:
        if isinstance(data["marches"], list):
            _data = data["marches"]
        elif isinstance(data["marches"], dict):
            if "marche" in data["marches"]:
                _data = data["marches"]["marche"]

    return detect_format_multiple_titulaires(_data, quorum=quorum)

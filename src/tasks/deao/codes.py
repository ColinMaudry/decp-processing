from collections import Counter

# Énumération de TYPE_PROCEDURE dans jo_types_diffusion.xsd (v3.2.5)
PROCEDURE_INDEXATION = {
    "OUVERT": "Appel d'offres ouvert",
    "RESTREINT": "Appel d'offres restreint",
    "NEGOCIE": "Procédure négociée",
    "PROCEDURE_ADAPTE": "Procédure adaptée",
    "DIALOGUE_COMPETITIF": "Dialogue compétitif",
    "PARTENARIAT_INNOVATION": "Partenariat d'innovation",
    "CONCOURS_OUVERT": "Concours ouvert",
    "CONCOURS_RESTREINT": "Concours restreint",
    "DSP": "Concession",
    "AUTRE": "Autre procédure",
}

PROCEDURE_EFORMS = {
    "open": "Appel d'offres ouvert",
    "restricted": "Appel d'offres restreint",
    "neg-w-call": "Procédure négociée",
    "neg-wo-call": "Procédure négociée",
    "comp-dial": "Dialogue compétitif",
    "innovation": "Partenariat d'innovation",
    "oth-single": "Autre procédure",
    "oth-mult": "Autre procédure",
}

TYPE_INDEXATION = {
    "TRAVAUX": "Travaux",
    "FOURNITURES": "Fournitures",
    "SERVICES": "Services",
}

TYPE_EFORMS = {"works": "Travaux", "supplies": "Fournitures", "services": "Services"}


def libelle(table: dict, code: str | None, compteur: Counter, nom: str) -> str | None:
    """Traduit un code ; un code inconnu est renvoyé tel quel et compté."""
    if code is None:
        return None
    if code in table:
        return table[code]
    compteur[f"non_mappe:{nom}:{code}"] += 1
    return code

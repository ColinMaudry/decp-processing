"""Accumulation des consultations d'une plateforme Atexo d'un passage à l'autre.

La fusion avec le fichier publié est commune aux plateformes : voir `plateformes`."""

from datetime import date

import polars as pl

from src.config import DEAO_STATUT_DELAI_JOURS
from src.tasks.deao.consolidate import statut_temporel_expr
from src.tasks.scrap.deao.atexo.dce import normaliser_taille
from src.tasks.scrap.deao.atexo.parse import ConsultationListe


def lots_a_relire(liste: list[ConsultationListe], precedent: pl.DataFrame) -> set[str]:
    """`ref_cons` dont la fenêtre des lots doit être (re)lue : nouveaux, date limite
    ou nombre de lots modifiés."""
    connus: dict[str, tuple] = {}
    if precedent.height:
        for (ref,), lignes in precedent.group_by(["id"]):
            connus[ref] = (
                lignes["dateLimiteReponse"][0],
                lignes.filter(pl.col("lotId") != "0").height,
            )
    a_relire = set()
    for c in liste:
        ancien = connus.get(c.ref_cons)
        if ancien is None or ancien[0] != c.date_limite or ancien[1] != c.nombre_lots:
            a_relire.add(c.ref_cons)
    return a_relire


def dce_a_telecharger(
    taille_affichee: str | None,
    taille_archivee: str | None,
    url_dce_precedente: str | None,
) -> bool:
    """Vrai si un DCE est en ligne et n'est pas déjà archivé à l'identique."""
    if taille_affichee is None:
        return False
    if taille_archivee is None or url_dce_precedente is None:
        return True
    return normaliser_taille(taille_affichee) != taille_archivee


def recherche_complete(
    nb_lus: int,
    nb_annonces: int,
    precedent: pl.DataFrame,
    today: date,
    delai_jours: int = DEAO_STATUT_DELAI_JOURS,
) -> bool:
    """Garde-fou : la recherche a tout lu et n'a pas renvoyé un nombre anormalement bas
    de consultations par rapport aux précédentes encore ouvertes à `today`."""
    ouvertes = 0
    if precedent.height:
        ouvertes = (
            precedent.filter(pl.col("statut") != "annule")
            .filter(statut_temporel_expr(today, delai_jours) == "ouvert")["id"]
            .n_unique()
        )
    return nb_lus == nb_annonces and nb_lus >= 0.5 * ouvertes

"""Accumulation des consultations d'une plateforme Atexo d'un passage à l'autre."""

from datetime import date, datetime, time

import polars as pl

from src.config import DEAO_STATUT_DELAI_JOURS
from src.tasks.deao.consolidate import statut_temporel_expr
from src.tasks.deao.schema import published_schema
from src.tasks.scrap.deao.atexo.dce import normaliser_taille
from src.tasks.scrap.deao.atexo.parse import FUSEAU, ConsultationListe


def _conformer(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col(nom).cast(dtype)
        if nom in df.columns
        else pl.lit(None, dtype=dtype).alias(nom)
        for nom, dtype in published_schema().items()
    )


def instant_du_passage(today: date, maintenant: datetime | None = None) -> datetime:
    """Instant auquel la liste est lue : maintenant pour un passage du jour, fin de la
    journée `today` pour un `run_date` explicite."""
    maintenant = (maintenant or datetime.now(FUSEAU)).astimezone(FUSEAU)
    if maintenant.date() == today:
        return maintenant
    return _fin_de_journee(today)


def _fin_de_journee(jour: date) -> datetime:
    return datetime.combine(jour, time.max, tzinfo=FUSEAU)


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


def fusionner(
    precedent: pl.DataFrame,
    nouvelles: pl.DataFrame,
    ids_vus: set[str],
    today: date,
    complete: bool,
    delai_jours: int = DEAO_STATUT_DELAI_JOURS,
    instant: datetime | None = None,
) -> pl.DataFrame:
    """Remplace les lignes des consultations relues, garde les autres et recalcule le statut.

    `instant` : moment de la lecture de la liste (défaut : fin de la journée `today`) ;
    une date limite passée à cet instant, même plus tôt dans la journée, clôt la
    consultation au lieu de l'annuler."""
    if instant is None:
        instant = _fin_de_journee(today)
    precedent = _conformer(precedent)
    nouvelles = _conformer(nouvelles)
    ids_nouveaux = nouvelles["id"].to_list()
    gardees = precedent.filter(~pl.col("id").is_in(ids_nouveaux))
    df = pl.concat(
        [gardees, nouvelles.with_columns(pl.lit(None, pl.String).alias("statut"))]
    )
    # statut précédent par consultation (celui des lignes remplacées compte aussi)
    statut_prec = precedent.group_by("id").agg(
        pl.col("statut").first().alias("statut_prec")
    )
    df = df.join(statut_prec, on="id", how="left")
    # Pour les lignes gardées, `statut` est déjà le statut précédent.
    prec = pl.coalesce(pl.col("statut_prec"), pl.col("statut"))
    limite = pl.col("dateLimiteReponse")
    echue = limite <= pl.lit(
        instant.astimezone(FUSEAU), dtype=df.schema["dateLimiteReponse"]
    )
    vu = pl.col("id").is_in(list(ids_vus))
    # Jamais « annule » sans date limite : dans le doute, on n'annule pas.
    annule = (pl.lit(complete) & limite.is_not_null() & ~echue & (prec != "clos")) | (
        prec == "annule"
    )
    df = df.with_columns(
        pl.when(~vu & annule.fill_null(False))
        .then(pl.lit("annule"))
        .when(echue.fill_null(False))
        .then(pl.lit("clos"))
        .otherwise(statut_temporel_expr(today, delai_jours))
        .alias("statut")
    )
    # `uid` doit rester unique (une consultation lue deux fois pendant la pagination).
    df = df.unique("uid", keep="last", maintain_order=True)
    return _conformer(df.drop("statut_prec")).sort("uid")

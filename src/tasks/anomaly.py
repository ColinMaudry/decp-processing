from pathlib import Path

import polars as pl
import polars.selectors as cs
from prefect.artifacts import create_markdown_artifact

from src.config import (
    ANOMALY_GROUPE_MIN_SIZE,
    ANOMALY_HABITANT_THRESHOLDS,
    ANOMALY_PAIRS_ABERRANT_THRESHOLD,
    ANOMALY_PAIRS_SUSPECT_THRESHOLD,
    ANOMALY_TITULAIRE_PME_MONTANT_SEUIL,
    DATA_DIR,
    POPULATION_COMMUNES_CSV,
)
from src.tasks.utils import logger


def make_short_cpv_code() -> pl.Expr:
    """Renvoie un code CPV plus court, dont la longueur dépend du code."""
    return (
        pl.when(pl.col("codeCPV").str.starts_with("45"))
        .then(pl.col("codeCPV").str.slice(0, 4))
        .otherwise(pl.col("codeCPV").str.slice(0, 3))
        .alias("codeCPV_court")
    )


def compute_tranche_population_expr(col: str = "population") -> pl.Expr:
    """Renvoie une expression polars qui calcule la tranche de population.

    Renvoie null si la population est null. Les bornes sont exclusives à droite :
    [0, 2000) → 'très petite', [2000, 10000) → 'petite', etc.
    """
    return (
        pl.when(pl.col(col).is_null())
        .then(None)
        .when(pl.col(col) < 2_000)
        .then(pl.lit("très petite"))
        .when(pl.col(col) < 10_000)
        .then(pl.lit("petite"))
        .when(pl.col(col) < 50_000)
        .then(pl.lit("moyenne"))
        .when(pl.col(col) < 200_000)
        .then(pl.lit("grande"))
        .otherwise(pl.lit("très grande"))
        .alias("tranche_population")
    )


def montant_normalise_expr() -> pl.Expr:
    """Normalise le montant par mois pour Services/Fournitures non forfaitaires.

    Pour les Travaux ou les marchés Forfaitaires, le montant est ponctuel et n'est
    pas divisé. Pour Services/Fournitures avec dureeMois > 1 et formePrix != 'Forfaitaire',
    on divise par la durée pour comparer des €/mois aux pairs.
    """
    return (
        pl.when(
            pl.col("type").is_in(["Services", "Fournitures"])
            & (pl.col("dureeMois") > 1)
            & (pl.col("formePrix") != "Forfaitaire")
        )
        .then(pl.col("montant") / pl.col("dureeMois"))
        .otherwise(pl.col("montant"))
        .alias("montant_normalise")
    )


def compute_peer_group_stats(
    lf: pl.LazyFrame,
    min_size: int,
    drop_columns: bool = True,
    lf_peers: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Calcule les statistiques médiane/MAD du log du montant normalisé par groupe de pairs.

    Quatre niveaux de granularité, du plus spécifique au plus large :
    - L4 : (cpv_2, type, acheteur_categorie, tranche_population)
    - L3 : (cpv_2, type, acheteur_categorie)
    - L2 : (type, acheteur_categorie)
    - L1 : (type,)

    Pour chaque marché, on retient les stats du niveau le plus spécifique dont le groupe
    a au moins `min_size` marchés. Si aucun niveau ne suffit, mediane_log/mad_log/median_montant_norm
    restent null et niveau_groupe est null.

    Le LazyFrame en entrée doit contenir les colonnes : codeCPV_court, type, acheteur_categorie,
    tranche_population, montant_normalise, log_montant_normalise.

    lf_peers : sous-ensemble de lf servant de base au calcul des médianes/MAD par groupe
        (typiquement les marchés actuels uniquement). Si None, lf sert à la fois de base
        statistique et de cible : c'est le comportement historique. Séparer les deux évite
        qu'un marché modifié plusieurs fois ne soit compté plusieurs fois dans les stats de
        son groupe de pairs, tout en laissant classifier toutes les lignes de lf (y compris
        l'historique des modifications) contre ces mêmes stats de référence.

    Renvoie le LazyFrame enrichi avec : n_groupe, niveau_groupe, mediane_log, mad_log,
    median_montant_norm.
    """
    if lf_peers is None:
        lf_peers = lf

    levels = [
        ("L4", ["codeCPV_court", "type", "acheteur_categorie", "tranche_population"]),
        ("L3", ["codeCPV_court", "type", "acheteur_categorie"]),
        ("L2", ["type", "acheteur_categorie"]),
        ("L1", ["type"]),
    ]

    for name, keys in levels:
        # Première passe : médiane par groupe (sur la base de pairs)
        mediane_stats = lf_peers.group_by(keys).agg(
            pl.len().alias(f"n_{name}"),
            pl.col("log_montant_normalise").median().alias(f"mediane_log_{name}"),
            pl.col("montant_normalise").median().alias(f"median_montant_norm_{name}"),
        )
        lf = lf.join(mediane_stats, on=keys, how="left")
        lf_peers = lf_peers.join(mediane_stats, on=keys, how="left")

        # Deuxième passe : MAD = médiane(|log - médiane_log|) par groupe (sur la base de pairs)
        mad_stats = lf_peers.group_by(keys).agg(
            (pl.col("log_montant_normalise") - pl.col(f"mediane_log_{name}"))
            .abs()
            .median()
            .alias(f"mad_log_{name}"),
        )
        lf = lf.join(mad_stats, on=keys, how="left")
        lf_peers = lf_peers.join(mad_stats, on=keys, how="left")

    lf = lf.with_columns(
        pl.coalesce(
            [
                pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"n_{name}"))
                for name, _ in levels
            ]
        ).alias("n_groupe"),
        pl.coalesce(
            [
                pl.when(pl.col(f"n_{name}") >= min_size).then(pl.lit(name))
                for name, _ in levels
            ]
        ).alias("niveau_groupe"),
        pl.coalesce(
            [
                pl.when(pl.col(f"n_{name}") >= min_size).then(
                    pl.col(f"mediane_log_{name}")
                )
                for name, _ in levels
            ]
        ).alias("mediane_log"),
        pl.coalesce(
            [
                pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"mad_log_{name}"))
                for name, _ in levels
            ]
        ).alias("mad_log"),
        pl.coalesce(
            [
                pl.when(pl.col(f"n_{name}") >= min_size).then(
                    pl.col(f"median_montant_norm_{name}")
                )
                for name, _ in levels
            ]
        ).alias("median_montant_norm"),
    )

    cols_to_drop = []
    if drop_columns:
        cols_to_drop = []
        for name, _ in levels:
            cols_to_drop += [
                f"n_{name}",
                f"mediane_log_{name}",
                f"mad_log_{name}",
                f"median_montant_norm_{name}",
            ]
        lf = lf.drop(cols_to_drop)
    return lf


def compute_signals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calcule ecart_pairs (Signal A) et montant_par_habitant (Signal B).

    ecart_pairs = (log_montant_normalise - mediane_log) / mad_log, null si mad_log nulle ou null.
    montant_par_habitant = montant / population, null si population null ou nulle.
    """
    return lf.with_columns(
        pl.when(pl.col("mad_log").is_null() | (pl.col("mad_log") == 0))
        .then(None)
        .otherwise(
            (pl.col("log_montant_normalise") - pl.col("mediane_log"))
            / pl.col("mad_log")
        )
        .alias("ecart_pairs"),
        pl.when(pl.col("population").is_null() | (pl.col("population") == 0))
        .then(None)
        .otherwise(pl.col("montant") / pl.col("population"))
        .alias("montant_par_habitant"),
    )


def join_population(lf: pl.LazyFrame, population_csv_path: Path) -> pl.LazyFrame:
    """Joint le LazyFrame avec le CSV des communes via le SIREN extrait de acheteur_id.

    SIREN = 9 premiers caractères du SIRET (acheteur_id est une chaîne).
    Renvoie le LazyFrame avec une nouvelle colonne 'population' (null si non trouvé).
    Si le fichier CSV est absent, la colonne 'population' est ajoutée avec des nulls.
    """
    if not population_csv_path.exists():
        logger.warning(
            f"Fichier population communes introuvable : {population_csv_path}. "
            "La colonne 'population' sera nulle (Signal B désactivé)."
        )
        return lf.with_columns(pl.lit(None).cast(pl.Int64).alias("population"))
    population_lf = pl.scan_csv(population_csv_path)
    # Paris, Lyon et Marseille partagent leur SIREN avec leurs arrondissements
    # (type=ARR, population vide) : on ne garde que la ligne de la commune (type=COM)
    # pour éviter un join qui duplique ces marchés et renvoie une population nulle.
    if "type" in population_lf.collect_schema().names():
        population_lf = population_lf.filter(pl.col("type") == "COM")
    population_lf = population_lf.select(
        pl.col("SIREN").cast(pl.Utf8),
        pl.col("population").cast(pl.Int64),
    )
    return (
        lf.with_columns(pl.col("acheteur_id").str.slice(0, 9).alias("_siren_acheteur"))
        .join(population_lf, left_on="_siren_acheteur", right_on="SIREN", how="left")
        .drop("_siren_acheteur")
    )


def classify_anomalies(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applique la logique de classification suspect/aberrant et collecte toutes les raisons.

    Le LazyFrame en entrée doit contenir : ecart_pairs, montant_par_habitant, type,
    titulaire_categorie, montant.

    Renvoie le LazyFrame enrichi avec montant_anomalie (null/suspect/aberrant) et
    montant_anomalie_raisons (List[String] de toutes les raisons déclenchées, null si aucune).

    Priorité des signaux : aberrant prime sur suspect. Le premier when
    (pairs_aberrant | habitant_aberrant → "aberrant") est évalué avant le second
    (pairs_suspect | habitant_suspect → "suspect"), donc aberrant a toujours la priorité
    même si plusieurs signaux sont actifs simultanément.
    """
    suspect_habitant_expr = (
        pl.when(pl.col("type") == "Travaux")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Travaux"]["suspect"]))
        .when(pl.col("type") == "Services")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Services"]["suspect"]))
        .when(pl.col("type") == "Fournitures")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Fournitures"]["suspect"]))
        .otherwise(None)
    )
    aberrant_habitant_expr = (
        pl.when(pl.col("type") == "Travaux")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Travaux"]["aberrant"]))
        .when(pl.col("type") == "Services")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Services"]["aberrant"]))
        .when(pl.col("type") == "Fournitures")
        .then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Fournitures"]["aberrant"]))
        .otherwise(None)
    )

    lf = lf.with_columns(
        pairs_aberrant=pl.col("ecart_pairs") > ANOMALY_PAIRS_ABERRANT_THRESHOLD,
        pairs_suspect=(pl.col("ecart_pairs") > ANOMALY_PAIRS_SUSPECT_THRESHOLD)
        & (pl.col("ecart_pairs") <= ANOMALY_PAIRS_ABERRANT_THRESHOLD),
        habitant_aberrant=pl.col("montant_par_habitant").is_not_null()
        & (pl.col("montant_par_habitant") > aberrant_habitant_expr),
        habitant_suspect=pl.col("montant_par_habitant").is_not_null()
        & (pl.col("montant_par_habitant") > suspect_habitant_expr)
        & ~(pl.col("montant_par_habitant") > aberrant_habitant_expr),
    )

    # aberrant prime sur suspect : le premier when gagne
    lf = lf.with_columns(
        classification_initiale=pl.when(
            pl.col("pairs_aberrant") | pl.col("habitant_aberrant")
        )
        .then(pl.lit("aberrant"))
        .when(pl.col("pairs_suspect") | pl.col("habitant_suspect"))
        .then(pl.lit("suspect"))
        .otherwise(None),
    )

    # Le montant est aberrant si au-dessus d'un certain seuil et attribbué à une PME seule
    lf = lf.with_columns(
        modulateur_titulaire=(pl.col("classification_initiale") == "suspect")
        & (pl.col("single_pme"))
        & (pl.col("montant") > ANOMALY_TITULAIRE_PME_MONTANT_SEUIL),
    )

    lf = lf.with_columns(
        montant_anomalie=pl.when(pl.col("modulateur_titulaire"))
        .then(pl.lit("aberrant"))
        .otherwise(pl.col("classification_initiale")),
    )

    lf = lf.with_columns(
        montant_anomalie_raisons=pl.concat_list(
            [
                pl.when(pl.col("habitant_aberrant"))
                .then(pl.lit("montant_par_habitant_aberrant"))
                .otherwise(None),
                pl.when(pl.col("habitant_suspect"))
                .then(pl.lit("montant_par_habitant_suspect"))
                .otherwise(None),
                pl.when(pl.col("pairs_aberrant"))
                .then(pl.lit("montant_vs_pairs_aberrant"))
                .otherwise(None),
                pl.when(pl.col("pairs_suspect"))
                .then(pl.lit("montant_vs_pairs_suspect"))
                .otherwise(None),
                pl.when(pl.col("modulateur_titulaire"))
                .then(pl.lit("titulaire_incoherent_pme_gros_marche"))
                .otherwise(None),
            ]
        ).list.drop_nulls()
    )
    lf = lf.with_columns(
        montant_anomalie_raisons=pl.when(pl.col("montant_anomalie").is_not_null())
        .then(pl.col("montant_anomalie_raisons"))
        .otherwise(None)
    )

    return lf.drop(
        [
            "pairs_aberrant",
            "pairs_suspect",
            "habitant_aberrant",
            "habitant_suspect",
            "classification_initiale",
            "modulateur_titulaire",
        ]
    )


def compute_montant_rationalise(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calcule montant_rationalise.

    - Si montant_anomalie != 'aberrant' → montant_rationalise = montant
    - Si montant_anomalie == 'aberrant' :
        - Si normalisation par durée appliquée (Services/Fournitures + dureeMois>1 + non-Forfaitaire) :
            montant_rationalise = median_montant_norm × dureeMois
        - Sinon : montant_rationalise = median_montant_norm (déjà en € totaux)
        - Si median_montant_norm null (groupe insuffisant) : montant_rationalise = null
    """
    normalisation_appliquee = (
        pl.col("type").is_in(["Services", "Fournitures"])
        & (pl.col("dureeMois") > 1)
        & (pl.col("formePrix") != "Forfaitaire")
    )

    return lf.with_columns(
        montant_rationalise=pl.when(
            pl.col("montant_anomalie").is_null()
            | (pl.col("montant_anomalie") != "aberrant")
        )
        .then(pl.col("montant"))
        .when(pl.col("median_montant_norm").is_null())
        .then(None)
        .when(normalisation_appliquee)
        .then(pl.col("median_montant_norm") * pl.col("dureeMois"))
        .otherwise(pl.col("median_montant_norm")),
    )


def build_anomaly_summary(lf: pl.LazyFrame) -> dict:
    """Construit un résumé des anomalies pour observabilité.

    Renvoie un dict avec : nb_suspects, nb_aberrants, sum_montant, sum_montant_rationalise,
    top_raisons (liste de dicts {raison, count}).
    """
    counts = (
        lf.select(
            nb_suspects=(pl.col("montant_anomalie") == "suspect").sum(),
            nb_aberrants=(pl.col("montant_anomalie") == "aberrant").sum(),
            sum_montant=pl.col("montant").sum().fill_null(0),
            sum_montant_rationalise=pl.col("montant_rationalise").sum().fill_null(0),
        )
        .collect()
        .to_dicts()[0]
    )

    top_raisons_df = (
        lf.filter(pl.col("montant_anomalie_raisons").is_not_null())
        .explode("montant_anomalie_raisons")
        .group_by("montant_anomalie_raisons")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(20)
        .collect()
    )

    return {
        "nb_suspects": int(counts["nb_suspects"]),
        "nb_aberrants": int(counts["nb_aberrants"]),
        "sum_montant": float(counts["sum_montant"]),
        "sum_montant_rationalise": float(counts["sum_montant_rationalise"]),
        "top_raisons": [
            {"raison": row["montant_anomalie_raisons"], "count": int(row["count"])}
            for row in top_raisons_df.to_dicts()
        ],
    }


def _create_anomaly_artifact(summary: dict) -> None:
    """Crée un artifact markdown Prefect avec le résumé des anomalies."""
    diff_pct = (
        100.0
        * (summary["sum_montant"] - summary["sum_montant_rationalise"])
        / summary["sum_montant"]
        if summary["sum_montant"] > 0
        else 0.0
    )
    raisons_md = "\n".join(
        f"- `{r['raison']}` : {r['count']}" for r in summary["top_raisons"]
    )
    md = f"""# Détection des anomalies de montant

## Comptage

- Marchés `suspect` : **{summary["nb_suspects"]}**
- Marchés `aberrant` : **{summary["nb_aberrants"]}**

## Impact sur les agrégations

- Somme des montants déclarés : **{summary["sum_montant"]:,.0f} €**
- Somme des montants rationalisés : **{summary["sum_montant_rationalise"]:,.0f} €**
- Différence : **{diff_pct:.2f} %**

## Top 20 raisons

{raisons_md}
"""

    logger.info(md)

    try:
        create_markdown_artifact(markdown=md, key="montant-anomalies-summary")
    except Exception:
        pass


def detect_montant_anomalies(
    lf: pl.LazyFrame,
    csv_path: Path | None = None,
    calibrating: bool = False,
) -> pl.LazyFrame:
    """Task Prefect : détecte les anomalies de montant et calcule montant_rationalise.

    Ajoute trois colonnes au LazyFrame en entrée :
    - montant_rationalise : montant utilisable pour les agrégations
    - montant_anomalie : null, 'suspect', ou 'aberrant'
    - montant_anomalie_raisons : code lisible du critère déclenché
    """

    # Une ligne = un marché (une version : actuelle ou une ancienne modification) où
    # on retire les données des titulaires pour fusionner les cotraitants.
    # Toutes les lignes (actuelles et historiques) sont classifiées : un montant
    # historique a lui aussi été réellement engagé et mérite d'être évalué.

    schema_names = lf.collect_schema().names()
    has_donnees_actuelles = "donneesActuelles" in schema_names

    # Comptage en streaming (pas de matérialisation du DataFrame) pour surveiller une
    # éventuelle démultiplication des lignes titulaires (cf. fusion des cotraitants).
    logger.info(f"height df avec titulaires: {lf.select(pl.len()).collect().item()}")

    # Identité complète d'une version de marché = toutes les colonnes non-titulaire.
    # (uid, modification_id) ne suffit PAS à identifier une ligne : des contrats
    # distincts peuvent partager le même (uid, modification_id) (collisions
    # d'identifiant, cf. #186), et un marché modifié plusieurs fois partage son uid
    # entre modifications. Réattacher les titulaires sur une clé trop grossière
    # produirait un produit cartésien. On calcule donc un identifiant de version _vid
    # (hash de toutes les colonnes non-titulaire) et on réattache les titulaires dessus.
    lf = lf.with_columns(_vid=pl.struct(~cs.starts_with("titulaire")).hash())

    lf_titulaires = lf.select("_vid", cs.starts_with("titulaire"))

    # On retire les colonnes titulaire_* (sauf titulaire_categorie, agrégée ci-dessous)
    # pour que le group_by fusionne bien les cotraitants d'une même version de marché.
    lf = lf.drop(cs.starts_with("titulaire") - cs.by_name("titulaire_categorie"))
    df = (
        lf.group_by(cs.exclude("titulaire_categorie"))
        .agg(pl.col("titulaire_categorie"))
        .with_columns(single_pme=pl.col("titulaire_categorie") == ["PME"])
        .drop("titulaire_categorie")
        .unique()
    ).collect(engine="streaming")

    lf = df.lazy()

    population_csv_path = csv_path or POPULATION_COMMUNES_CSV
    lf = join_population(lf, population_csv_path)

    lf = lf.with_columns(
        compute_tranche_population_expr(),
        montant_normalise_expr(),
        make_short_cpv_code(),
    )
    lf = lf.with_columns(
        log_montant_normalise=(pl.col("montant_normalise") + 1).log10(),
    )

    # Les stats de pairs (médiane/MAD par groupe) sont calculées uniquement sur les
    # marchés actuels : un marché modifié plusieurs fois ne doit pas peser plusieurs
    # fois dans le calcul des seuils de son groupe. Mais la classification elle-même
    # (ci-dessous) s'applique à toutes les lignes, actuelles et historiques.
    lf_peers = lf.filter(pl.col("donneesActuelles")) if has_donnees_actuelles else lf

    lf = compute_peer_group_stats(
        lf, min_size=ANOMALY_GROUPE_MIN_SIZE, lf_peers=lf_peers
    )
    lf = compute_signals(lf)
    lf = classify_anomalies(lf)
    lf = compute_montant_rationalise(lf)

    intermediaire = [
        "tranche_population",
        "montant_normalise",
        "log_montant_normalise",
        "codeCPV_court",
        "n_groupe",
        "niveau_groupe",
        "mediane_log",
        "mad_log",
        "median_montant_norm",
        "ecart_pairs",
        "montant_par_habitant",
        "single_pme",
        "population",
    ]

    # Build summary and create Prefect artifact
    if not calibrating:
        summary = build_anomaly_summary(
            lf.select(
                "montant",
                "montant_rationalise",
                "montant_anomalie",
                "montant_anomalie_raisons",
            )
        )
        _create_anomaly_artifact(summary)

    # On conserve la population
    lf = lf.with_columns(acheteur_population=pl.col("population").cast(pl.Int32))

    if not calibrating:
        lf = lf.drop([c for c in intermediaire if c in lf.collect_schema().names()])
        # On remet les données titulaires en s'appuyant sur l'identité complète de la
        # version de marché (_vid), et non sur (uid, modification_id) : cela évite le
        # produit cartésien quand des contrats distincts partagent (uid, modification_id).
        lf = lf.join(lf_titulaires, how="left", on="_vid").drop("_vid")
        # Checkpoint sur disque plutôt qu'en RAM : le résultat des anomalies est relu
        # en streaming par l'aval (stats, NAF/CPV, sink final) au lieu de conserver tout
        # le DataFrame déplié (titulaires réintégrés) en mémoire jusqu'à la fin du flow.
        anomalies_path = DATA_DIR / "temp" / "decp_anomalies.parquet"
        anomalies_path.parent.mkdir(parents=True, exist_ok=True)
        lf.sink_parquet(anomalies_path, engine="streaming")
        lf = pl.scan_parquet(anomalies_path)

        # Doit désormais correspondre à "height df avec titulaires" (plus de
        # démultiplication). Lu depuis les métadonnées du parquet : quasi gratuit.
        logger.info(f"height df à la fin: {lf.select(pl.len()).collect().item()}")

    # _vid est interne : on le retire (déjà fait après la re-jointure hors calibrage).
    lf = lf.drop("_vid", strict=False)

    return lf

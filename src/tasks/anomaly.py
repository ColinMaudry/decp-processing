from pathlib import Path

import polars as pl

from src.config import (
    ANOMALY_HABITANT_THRESHOLDS,
    ANOMALY_PAIRS_ABERRANT_THRESHOLD,
    ANOMALY_PAIRS_SUSPECT_THRESHOLD,
    ANOMALY_TITULAIRE_PME_MONTANT_SEUIL,
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


def compute_peer_group_stats(lf: pl.LazyFrame, min_size: int) -> pl.LazyFrame:
    """Calcule les statistiques médiane/MAD du log du montant normalisé par groupe de pairs.

    Quatre niveaux de granularité, du plus spécifique au plus large :
    - L4 : (cpv_2, type, acheteur_categorie, tranche_population)
    - L3 : (cpv_2, type, acheteur_categorie)
    - L2 : (type, acheteur_categorie)
    - L1 : (type,)

    Pour chaque marché, on retient les stats du niveau le plus spécifique dont le groupe
    a au moins `min_size` marchés. Si aucun niveau ne suffit, mediane_log/mad_log/median_montant_norm
    restent null et niveau_groupe est null.

    Le LazyFrame en entrée doit contenir les colonnes : codeCPV_2, type, acheteur_categorie,
    tranche_population, montant_normalise, log_montant_normalise.

    Renvoie le LazyFrame enrichi avec : n_groupe, niveau_groupe, mediane_log, mad_log,
    median_montant_norm.
    """
    levels = [
        ("L4", ["codeCPV_2", "type", "acheteur_categorie", "tranche_population"]),
        ("L3", ["codeCPV_2", "type", "acheteur_categorie"]),
        ("L2", ["type", "acheteur_categorie"]),
        ("L1", ["type"]),
    ]

    for name, keys in levels:
        # Première passe : médiane par groupe
        mediane_stats = lf.group_by(keys).agg(
            pl.len().alias(f"n_{name}"),
            pl.col("log_montant_normalise").median().alias(f"mediane_log_{name}"),
            pl.col("montant_normalise").median().alias(f"median_montant_norm_{name}"),
        )
        lf = lf.join(mediane_stats, on=keys, how="left")

        # Deuxième passe : MAD = médiane(|log - médiane_log|) par groupe
        mad_stats = lf.group_by(keys).agg(
            (pl.col("log_montant_normalise") - pl.col(f"mediane_log_{name}"))
            .abs()
            .median()
            .alias(f"mad_log_{name}"),
        )
        lf = lf.join(mad_stats, on=keys, how="left")

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
    for name, _ in levels:
        cols_to_drop += [
            f"n_{name}",
            f"mediane_log_{name}",
            f"mad_log_{name}",
            f"median_montant_norm_{name}",
        ]
    return lf.drop(cols_to_drop)


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
    """
    population_lf = pl.scan_csv(population_csv_path).select(
        pl.col("SIREN").cast(pl.Utf8),
        pl.col("population").cast(pl.Int64),
    )
    return (
        lf.with_columns(pl.col("acheteur_id").str.slice(0, 9).alias("_siren_acheteur"))
        .join(population_lf, left_on="_siren_acheteur", right_on="SIREN", how="left")
        .drop("_siren_acheteur")
    )


def classify_anomalies(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applique la logique de classification suspect/aberrant et choisit la raison principale.

    Le LazyFrame en entrée doit contenir : ecart_pairs, montant_par_habitant, type,
    titulaire_categorie, montant.

    Renvoie le LazyFrame enrichi avec montant_anomalie (null/suspect/aberrant) et
    montant_anomalie_raison.

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

    lf = lf.with_columns(
        modulateur_titulaire=(pl.col("classification_initiale") == "suspect")
        & (pl.col("titulaire_categorie") == "PME")
        & (pl.col("montant") > ANOMALY_TITULAIRE_PME_MONTANT_SEUIL),
    )

    lf = lf.with_columns(
        montant_anomalie=pl.when(pl.col("modulateur_titulaire"))
        .then(pl.lit("aberrant"))
        .otherwise(pl.col("classification_initiale")),
    )

    lf = lf.with_columns(
        montant_anomalie_raison=pl.when(pl.col("montant_anomalie").is_null())
        .then(None)
        .when(pl.col("habitant_aberrant"))
        .then(pl.lit("montant_par_habitant_aberrant"))
        .when(pl.col("habitant_suspect") & (pl.col("montant_anomalie") == "suspect"))
        .then(pl.lit("montant_par_habitant_suspect"))
        .when(pl.col("pairs_aberrant"))
        .then(pl.lit("montant_vs_pairs_aberrant"))
        .when(pl.col("pairs_suspect") & (pl.col("montant_anomalie") == "suspect"))
        .then(pl.lit("montant_vs_pairs_suspect"))
        .when(pl.col("modulateur_titulaire"))
        .then(pl.lit("titulaire_incoherent_pme_gros_marche"))
        .otherwise(None),
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

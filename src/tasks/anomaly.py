from pathlib import Path

import polars as pl


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

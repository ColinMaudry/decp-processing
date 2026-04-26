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

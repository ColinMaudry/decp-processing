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

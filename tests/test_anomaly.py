import polars as pl

from src.tasks.anomaly import compute_tranche_population_expr


class TestTranchePopulation:
    def test_tranches_bornes(self):
        lf_in = pl.LazyFrame(
            {
                "population": [
                    None,
                    0,
                    1_999,
                    2_000,
                    9_999,
                    10_000,
                    49_999,
                    50_000,
                    199_999,
                    200_000,
                    1_000_000,
                ],
            }
        )
        lf_out = lf_in.with_columns(compute_tranche_population_expr())
        result = lf_out.collect()["tranche_population"].to_list()
        assert result == [
            None,
            "très petite",
            "très petite",
            "petite",
            "petite",
            "moyenne",
            "moyenne",
            "grande",
            "grande",
            "très grande",
            "très grande",
        ]

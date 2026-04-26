import polars as pl

from src.tasks.anomaly import compute_tranche_population_expr, montant_normalise_expr


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


class TestMontantNormalise:
    def test_normalisation_par_duree_pour_services_non_forfaitaire(self):
        lf = pl.LazyFrame(
            {
                "montant": [120_000.0, 120_000.0, 120_000.0, 120_000.0, 120_000.0],
                "dureeMois": [12, 12, 12, 0, 1],
                "type": ["Services", "Services", "Travaux", "Services", "Services"],
                "formePrix": [
                    "Unitaire",
                    "Forfaitaire",
                    "Unitaire",
                    "Unitaire",
                    "Unitaire",
                ],
            }
        )
        result = (
            lf.with_columns(montant_normalise_expr())
            .collect()["montant_normalise"]
            .to_list()
        )
        assert result == [10_000.0, 120_000.0, 120_000.0, 120_000.0, 120_000.0]

    def test_normalisation_avec_montant_null(self):
        lf = pl.LazyFrame(
            {
                "montant": [None],
                "dureeMois": [12],
                "type": ["Services"],
                "formePrix": ["Unitaire"],
            },
            schema={
                "montant": pl.Float64,
                "dureeMois": pl.Int64,
                "type": pl.Utf8,
                "formePrix": pl.Utf8,
            },
        )
        result = (
            lf.with_columns(montant_normalise_expr())
            .collect()["montant_normalise"]
            .to_list()
        )
        assert result == [None]

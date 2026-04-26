import polars as pl

from src.config import BASE_DIR
from src.tasks.anomaly import (
    compute_peer_group_stats,
    compute_tranche_population_expr,
    join_population,
    montant_normalise_expr,
)


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


class TestJoinPopulation:
    def test_join_population_via_siren(self):
        # acheteur_id = SIRET (14 chiffres, string). SIREN = 9 premiers chiffres.
        lf = pl.LazyFrame(
            {
                "acheteur_id": [
                    "21005400200012",
                    "21013055700019",
                    "12345678900012",
                    None,
                ],
                "uid": ["A", "B", "C", "D"],
            },
            schema={"acheteur_id": pl.Utf8, "uid": pl.Utf8},
        )

        csv_path = BASE_DIR / "tests/data/identifiants-communes-test.csv"
        result = join_population(lf, csv_path).collect()

        # Vérifier l'ordre par uid (le join peut le perturber)
        result = result.sort("uid")
        assert result["population"].to_list() == [520000, 870000, None, None]


class TestPeerGroupStats:
    def test_stats_l3_groupe_suffisant(self):
        import math

        normaux = [100_000.0 * math.exp((i % 10 - 5) * 0.1) for i in range(35)]
        montants = normaux + [100_000_000.0]

        lf = pl.LazyFrame(
            {
                "uid": [f"U{i}" for i in range(36)],
                "montant_normalise": montants,
                "log_montant_normalise": [math.log10(m + 1) for m in montants],
                "codeCPV_2": ["45"] * 36,
                "type": ["Travaux"] * 36,
                "acheteur_categorie": ["Commune"] * 36,
                "tranche_population": [None] * 36,
            }
        )

        result = compute_peer_group_stats(lf, min_size=30).collect()

        assert (result["n_groupe"] == 36).all()
        assert (result["niveau_groupe"] == "L3").all()
        median_log = result["mediane_log"].to_list()[0]
        assert 4.9 < median_log < 5.1

    def test_stats_groupe_insuffisant(self):
        lf = pl.LazyFrame(
            {
                "uid": [f"U{i}" for i in range(5)],
                "montant_normalise": [100.0] * 5,
                "log_montant_normalise": [2.0] * 5,
                "codeCPV_2": ["99"] * 5,
                "type": ["Fournitures"] * 5,
                "acheteur_categorie": ["État"] * 5,
                "tranche_population": [None] * 5,
            }
        )
        result = compute_peer_group_stats(lf, min_size=30).collect()
        assert result["niveau_groupe"].to_list() == [None] * 5
        assert result["mediane_log"].to_list() == [None] * 5

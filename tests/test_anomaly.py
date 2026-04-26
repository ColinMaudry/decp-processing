import polars as pl

from src.config import (
    BASE_DIR,
)
from src.tasks.anomaly import (
    build_anomaly_summary,
    classify_anomalies,
    compute_montant_rationalise,
    compute_peer_group_stats,
    compute_signals,
    compute_tranche_population_expr,
    detect_montant_anomalies,
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


class TestSignals:
    def test_ecart_pairs_calcul(self):
        lf = pl.LazyFrame(
            {
                "montant_normalise": [100_000.0, 100_000_000.0, 100_000.0],
                "log_montant_normalise": [5.0, 8.0, 5.0],
                "mediane_log": [5.0, 5.0, None],  # Dernier : groupe insuffisant
                "mad_log": [0.5, 0.5, None],
                "montant": [100_000.0, 100_000_000.0, 100_000.0],
                "population": [10_000, 10_000, None],
                "type": ["Services", "Services", "Services"],
            }
        )
        result = compute_signals(lf).collect()
        assert result["ecart_pairs"].to_list() == [0.0, 6.0, None]
        assert result["montant_par_habitant"].to_list() == [10.0, 10_000.0, None]

    def test_ecart_pairs_mad_zero_renvoie_null(self):
        lf = pl.LazyFrame(
            {
                "montant_normalise": [100_000.0],
                "log_montant_normalise": [5.0],
                "mediane_log": [5.0],
                "mad_log": [0.0],
                "montant": [100_000.0],
                "population": [None],
                "type": ["Services"],
            },
            schema_overrides={"population": pl.Int64},
        )
        result = compute_signals(lf).collect()
        assert result["ecart_pairs"].to_list() == [None]


class TestClassification:
    def test_pairs_suspect(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [4.5],
                "montant_par_habitant": [None],
                "type": ["Services"],
                "titulaire_categorie": [None],
                "montant": [100_000.0],
            },
            schema_overrides={"montant_par_habitant": pl.Float64},
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["suspect"]
        assert result["montant_anomalie_raison"].to_list() == [
            "montant_vs_pairs_suspect"
        ]

    def test_pairs_aberrant(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [10.0],
                "montant_par_habitant": [None],
                "type": ["Services"],
                "titulaire_categorie": [None],
                "montant": [100_000.0],
            },
            schema_overrides={"montant_par_habitant": pl.Float64},
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == [
            "montant_vs_pairs_aberrant"
        ]

    def test_habitant_aberrant_prime_sur_pairs(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [10.0],
                "montant_par_habitant": [25_000.0],  # Travaux aberrant > 20 000
                "type": ["Travaux"],
                "titulaire_categorie": [None],
                "montant": [100_000_000.0],
            }
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == [
            "montant_par_habitant_aberrant"
        ]

    def test_modulateur_pme_escalade_suspect_en_aberrant(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [4.5],
                "montant_par_habitant": [None],
                "type": ["Services"],
                "titulaire_categorie": ["PME"],
                "montant": [60_000_000.0],
            },
            schema_overrides={"montant_par_habitant": pl.Float64},
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == [
            "titulaire_incoherent_pme_gros_marche"
        ]

    def test_modulateur_pme_inactif_si_montant_sous_seuil(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [4.5],
                "montant_par_habitant": [None],
                "type": ["Services"],
                "titulaire_categorie": ["PME"],
                "montant": [10_000_000.0],  # < 50 M€
            },
            schema_overrides={"montant_par_habitant": pl.Float64},
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["suspect"]
        assert result["montant_anomalie_raison"].to_list() == [
            "montant_vs_pairs_suspect"
        ]

    def test_pas_d_anomalie(self):
        lf = pl.LazyFrame(
            {
                "ecart_pairs": [2.0],
                "montant_par_habitant": [100.0],
                "type": ["Services"],
                "titulaire_categorie": ["GE"],
                "montant": [50_000.0],
            }
        )
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == [None]
        assert result["montant_anomalie_raison"].to_list() == [None]


class TestMontantRationalise:
    def test_aberrant_remplace_par_mediane_x_duree(self):
        # Services + Unitaire + 12 mois → normalisation appliquée → rationalise = 8000*12
        lf = pl.LazyFrame(
            {
                "montant": [10_000_000.0],
                "montant_anomalie": ["aberrant"],
                "median_montant_norm": [8_000.0],
                "type": ["Services"],
                "dureeMois": [12],
                "formePrix": ["Unitaire"],
            }
        )
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [96_000.0]

    def test_aberrant_travaux_pas_de_normalisation(self):
        lf = pl.LazyFrame(
            {
                "montant": [10_000_000.0],
                "montant_anomalie": ["aberrant"],
                "median_montant_norm": [500_000.0],
                "type": ["Travaux"],
                "dureeMois": [24],
                "formePrix": ["Forfaitaire"],
            }
        )
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [500_000.0]

    def test_suspect_garde_montant_origine(self):
        lf = pl.LazyFrame(
            {
                "montant": [10_000_000.0],
                "montant_anomalie": ["suspect"],
                "median_montant_norm": [8_000.0],
                "type": ["Services"],
                "dureeMois": [12],
                "formePrix": ["Unitaire"],
            }
        )
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [10_000_000.0]

    def test_pas_d_anomalie_garde_montant(self):
        lf = pl.LazyFrame(
            {
                "montant": [50_000.0],
                "montant_anomalie": [None],
                "median_montant_norm": [None],
                "type": ["Services"],
                "dureeMois": [None],
                "formePrix": [None],
            },
            schema_overrides={
                "montant_anomalie": pl.Utf8,
                "median_montant_norm": pl.Float64,
                "dureeMois": pl.Int64,
                "formePrix": pl.Utf8,
            },
        )
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [50_000.0]

    def test_aberrant_groupe_insuffisant_donne_null(self):
        lf = pl.LazyFrame(
            {
                "montant": [10_000_000.0],
                "montant_anomalie": ["aberrant"],
                "median_montant_norm": [None],
                "type": ["Services"],
                "dureeMois": [12],
                "formePrix": ["Unitaire"],
            },
            schema_overrides={"median_montant_norm": pl.Float64},
        )
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [None]


class TestAnomalySummary:
    def test_summary_compte_aberrants_et_suspects(self):
        lf = pl.LazyFrame(
            {
                "montant": [100.0, 200.0, 300.0, 400.0, 500.0],
                "montant_rationalise": [100.0, 200.0, 50.0, 400.0, 500.0],
                "montant_anomalie": ["suspect", None, "aberrant", "aberrant", None],
                "montant_anomalie_raison": [
                    "montant_vs_pairs_suspect",
                    None,
                    "montant_par_habitant_aberrant",
                    "montant_par_habitant_aberrant",
                    None,
                ],
            }
        )
        summary = build_anomaly_summary(lf)
        assert summary["nb_suspects"] == 1
        assert summary["nb_aberrants"] == 2
        assert summary["sum_montant"] == 1500.0
        assert summary["sum_montant_rationalise"] == 1250.0
        assert summary["top_raisons"][0]["raison"] == "montant_par_habitant_aberrant"
        assert summary["top_raisons"][0]["count"] == 2


class TestDetectMontantAnomalies:
    def test_pipeline_complet_sur_petit_dataset(self):
        import math

        n_normaux = 35
        normaux = [
            {
                "uid": f"N{i}",
                "acheteur_id": "21005400200012",  # Lyon SIRET (string)
                "montant": 100_000.0 * math.exp((i % 5 - 2) * 0.1),
                "type": "Services",
                "codeCPV": "72000000",
                "dureeMois": 12,
                "formePrix": "Unitaire",
                "acheteur_categorie": "Commune",
                "titulaire_categorie": "GE",
            }
            for i in range(n_normaux)
        ]

        outlier = [
            {
                "uid": "OUTLIER",
                "acheteur_id": "21005400200012",
                "montant": 10_000_000_000.0,  # 10 G€
                "type": "Services",
                "codeCPV": "72000000",
                "dureeMois": 12,
                "formePrix": "Unitaire",
                "acheteur_categorie": "Commune",
                "titulaire_categorie": "GE",
            }
        ]

        lf = pl.LazyFrame(normaux + outlier)

        csv_path = BASE_DIR / "tests/data/identifiants-communes-test.csv"
        result = detect_montant_anomalies(lf, csv_path).collect()

        # Must add exactly 3 columns
        added = set(result.columns) - set(lf.collect().columns)
        assert added == {
            "montant_rationalise",
            "montant_anomalie",
            "montant_anomalie_raison",
        }

        # Outlier must be aberrant
        outlier_row = result.filter(pl.col("uid") == "OUTLIER").to_dicts()[0]
        assert outlier_row["montant_anomalie"] == "aberrant"
        assert outlier_row["montant_rationalise"] is not None
        assert outlier_row["montant_rationalise"] < 1_000_000

        # Normal markets must not be flagged
        normaux_anomalies = result.filter(pl.col("uid") != "OUTLIER")[
            "montant_anomalie"
        ].to_list()
        assert all(a is None for a in normaux_anomalies)
        normaux_check = result.filter(pl.col("uid") != "OUTLIER")
        assert (normaux_check["montant"] == normaux_check["montant_rationalise"]).all()

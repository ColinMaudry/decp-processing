import polars as pl
from polars.testing import assert_frame_equal

from src.config import BASE_DIR, DECP_COLMO_DATASET, DECP_COLMO_RESOURCE_URL
from src.tasks.transform import (
    apply_modifications,
    calculate_naf_cpv_matching,
    consolidate_across_datasets,
    join_population,
    prepare_etablissements,
    prepare_unites_legales,
    sort_modifications,
)


class TestPrepareUnitesLegales:
    def test_prepare_unites_legales(self):
        lf = pl.LazyFrame(
            [
                # Cas 1: Personne morale
                {
                    "siren": "111111111",
                    "denominationUniteLegale": "Org 1",
                    "prenomUsuelUniteLegale": None,
                    "nomUniteLegale": None,
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "ETI",
                    "categorieJuridiqueUniteLegale": "1234",
                },
                # Cas 2: Personne physique avec nom d'usage
                {
                    "siren": "222222222",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": "Zacroit",  # a la priorité
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": "1234",
                },
                # Cas 3: Personne physique sans nom d'usage
                {
                    "siren": "333333333",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "PME",
                },
                # Cas 4: Nom non-diffusible
                {
                    "siren": "44444444",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "P",
                    "categorieEntreprise": "PME",
                },
            ]
        )

        # Expected DataFrame
        expected_df = pl.DataFrame(
            [
                # Cas 1: denominationUniteLegale est préservé
                {
                    "siren": "111111111",
                    "denominationUniteLegale": "Org 1",
                    "categorieEntreprise": "ETI",
                    "categorieJuridiqueUniteLegale": "1234",
                },
                # Cas 2: denominationUniteLegale = prenom + nomUsage (Zacroit)
                {
                    "siren": "222222222",
                    "denominationUniteLegale": "Ambroise Zacroit",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": "1234",
                },
                # Cas 3: denominationUniteLegale = prenom + nom (Croizat)
                {
                    "siren": "333333333",
                    "denominationUniteLegale": "Ambroise Croizat",
                    "categorieEntreprise": "PME",
                },
                # Cas 4: denominationUniteLegale = non-diffusible
                {
                    "siren": "44444444",
                    "denominationUniteLegale": "[Données personnelles non-diffusibles]",
                    "categorieEntreprise": "PME",
                },
            ]
        )

        # Application de la fonction
        result_df = prepare_unites_legales(lf).collect()

        # Tri des df
        result_df = result_df.sort("siren")
        expected_df = expected_df.sort("siren")

        assert_frame_equal(result_df, expected_df)


class TestPrepareEtablissements:
    def test_prepare_etablissements(self):
        lf = pl.LazyFrame(
            [
                {
                    "siret": "11111111111",
                    "codeCommuneEtablissement": "1053",
                    "enseigne1Etablissement": None,
                    "denominationUsuelleEtablissement": "Dénom usuelle",
                    "activitePrincipaleEtablissement": "11.1A",
                    "nomenclatureActivitePrincipaleEtablissement": "NAFv2",
                }
            ]
        )

        expected_df = pl.DataFrame(
            [
                {
                    "siret": "00011111111111",
                    "commune_code": "01053",
                    "etablissement_nom": "Dénom usuelle",
                    "activite_code": "11.1A",
                    "activite_nomenclature": "NAFv2",
                    "commune_nom": "Bourg-en-Bresse",
                    "departement_code": "01",
                    "region_code": "84",
                    "region_nom": "Auvergne-Rhône-Alpes",
                    "departement_nom": "Ain",
                }
            ]
        )

        assert_frame_equal(
            prepare_etablissements(lf).collect(),
            expected_df,
            check_column_order=False,
            check_dtypes=True,
        )


class TestHandleModificationsMarche:
    def test_apply_modifications(self):
        # Input LazyFrame - 3 test cases covering key scenarios
        lf = pl.LazyFrame(
            [
                # Case 1: uid=1 with 2 modifications (changes to montant, dureeMois, titulaires)
                {
                    "uid": "1",
                    "montant": 1000,
                    "dureeMois": 12,
                    "acheteur_id": "12345",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}}
                    ],
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "modification_dateNotificationModification": None,
                    "modification_datePublicationDonneesModification": None,
                    "modification_montant": None,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
                {
                    "uid": "1",
                    "montant": 1000,
                    "dureeMois": 12,
                    "acheteur_id": "12345",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}}
                    ],
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "modification_dateNotificationModification": "2023-02-04",
                    "modification_datePublicationDonneesModification": "2023-02-05",
                    "modification_montant": 1500,
                    "modification_dureeMois": 18,
                    "modification_titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                },
                {
                    "uid": "1",
                    "montant": 1000,
                    "dureeMois": 12,
                    "acheteur_id": "12345",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}}
                    ],
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "modification_dateNotificationModification": "2023-03-04",
                    "modification_datePublicationDonneesModification": "2023-03-05",
                    "modification_montant": None,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
                # Case 2: uid=2 with no modifications (all modification fields are None)
                {
                    "uid": "2",
                    "acheteur_id": "99999",
                    "montant": 500,
                    "dureeMois": 6,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}
                    ],
                    "dateNotification": "2023-03-01",
                    "datePublicationDonnees": "2023-03-02",
                    "modification_dateNotificationModification": None,
                    "modification_datePublicationDonneesModification": None,
                    "modification_montant": None,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
            ]
        )

        # Expected DataFrame
        expected_df = pl.DataFrame(
            [
                # uid=1: 2 rows (original + 1 modification)
                {
                    "uid": "1",
                    "dateNotification": "2023-03-04",
                    "datePublicationDonnees": "2023-03-05",
                    "montant": 1500,
                    "dureeMois": 18,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "acheteur_id": "12345",
                },
                {
                    "uid": "1",
                    "dateNotification": "2023-02-04",
                    "datePublicationDonnees": "2023-02-05",
                    "montant": 1500,
                    "dureeMois": 18,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "acheteur_id": "12345",
                },
                {
                    "uid": "1",
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "montant": 1000,
                    "dureeMois": 12,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                    ],
                    "acheteur_id": "12345",
                },
                # uid=2: 1 row (no modifications)
                {
                    "uid": "2",
                    "dateNotification": "2023-03-01",
                    "datePublicationDonnees": "2023-03-02",
                    "montant": 500,
                    "dureeMois": 6,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}
                    ],
                    "acheteur_id": "99999",
                },
            ]
        )

        # Call the function
        result_df = apply_modifications(lf).collect()

        sort_by = ["uid", "dateNotification"]

        # Assert the result matches the expected DataFrame
        assert_frame_equal(
            result_df.sort(by=sort_by),
            expected_df.sort(by=sort_by),
            check_column_order=False,
            check_dtypes=False,
        )

    def test_sort_modifications(self):
        """
        Générée par la LLM Euria, développée et hébergée en Suisse par Infomaniak. Vérifiée par l'auteur.
        """
        df_input = pl.DataFrame(
            {
                "uid": ["A", "A", "A", "B", "B"],
                "dateNotification": [
                    "2023-01-01",
                    "2023-01-03",
                    "2023-01-02",
                    "2023-02-01",
                    "2023-02-02",
                ],
                "montant": [100.0, 300.0, 300.0, 500.0, 500.0],
                "dureeMois": [12, 24, 24, 12, 36],
                "titulaire_id": ["T1", "T3", "T3", "T5", "T5"],
                "titulaire_typeIdentifiant": ["ID", "ID", "ID", "ID", "ID"],
            }
        ).with_columns(
            pl.col("dateNotification").str.strptime(
                pl.Date, format="%Y-%m-%d", strict=False
            )
        )

        # Appliquer la fonction
        result = sort_modifications(df_input.lazy()).collect()

        expected = pl.DataFrame(
            {
                "uid": ["A", "A", "A", "B", "B"],
                "dateNotification": [
                    "2023-01-03",
                    "2023-01-02",
                    "2023-01-01",
                    "2023-02-02",
                    "2023-02-01",
                ],
                "montant": [300.0, 300.0, 100.0, 500.0, 500.0],
                "dureeMois": [24, 24, 12, 36, 12],
                "titulaire_id": ["T3", "T3", "T1", "T5", "T5"],
                "titulaire_typeIdentifiant": ["ID", "ID", "ID", "ID", "ID"],
                "modification_id": [2, 1, 0, 1, 0],
                "donneesActuelles": [True, False, False, True, False],
            }
        ).with_columns(
            pl.col("dateNotification").str.strptime(
                pl.Date, format="%Y-%m-%d", strict=False
            )
        )

        # Comparaison stricte (ordre des colonnes, types, valeurs)
        assert_frame_equal(
            result,
            expected,
            check_dtype=False,
            check_exact=True,
            check_column_order=False,
            check_row_order=True,
        )


class TestCalculateNafCpvMatching:
    @staticmethod
    def _capture_output(monkeypatch):
        """Capture le DataFrame de résultats au lieu de l'écrire sur disque."""
        captured = {}

        def fake_save(df, path, file_format=None):
            captured["df"] = df

        monkeypatch.setattr("src.tasks.transform.save_to_files", fake_save)
        return captured

    def test_handles_no_naf_cpv_pairs_without_crashing(self, monkeypatch):
        captured = self._capture_output(monkeypatch)
        # Aucune ligne n'a à la fois un NAF et un CPV exploitables :
        # le pivot ne produit aucune colonne CPV.
        lf = pl.LazyFrame(
            {
                "uid": ["M1", "M2"],
                "codeCPV": ["45000000", "45000000"],
                "titulaire_activite_code": [None, None],
                "titulaire_activite_nomenclature": [None, None],
                "donneesActuelles": [True, True],
            },
            schema_overrides={
                "titulaire_activite_code": pl.String,
                "titulaire_activite_nomenclature": pl.String,
            },
        )

        calculate_naf_cpv_matching(lf)  # ne doit pas lever

        assert captured["df"].is_empty()

    def test_keeps_marche_naf_even_when_a_titulaire_has_no_naf(self, monkeypatch):
        captured = self._capture_output(monkeypatch)
        n = 30
        # Chaque marché a 2 titulaires (tous donneesActuelles) : un sans NAF, un avec.
        # La ligne SANS NAF est placée en premier : .unique("uid") sans tri la
        # garderait, écartant arbitrairement la ligne porteuse du NAF.
        # Tous les marchés doivent malgré tout être comptés pour la paire
        # (43.21B, 45000000).
        lf = pl.LazyFrame(
            {
                "uid": [f"M{i}" for i in range(n)] * 2,
                "codeCPV": ["45000000"] * (2 * n),
                "titulaire_activite_code": [None] * n + ["43.21B"] * n,
                "titulaire_activite_nomenclature": [None] * n + ["NAFREV2"] * n,
                "donneesActuelles": [True] * (2 * n),
            }
        )

        calculate_naf_cpv_matching(lf)
        df = captured["df"]

        pair = df.filter(
            (pl.col("activite_code") == "43.21B") & (pl.col("cpv") == "45000000")
        )
        assert pair.height == 1
        assert pair["nb_marches"].item() == n

    def test_scores_ranks_et_nb_marches(self, monkeypatch):
        """Verrouille le calcul de la probabilité conditionnelle P(cpv|naf),
        du rang et du nombre de marchés par paire NAF/CPV.

        NAF A (01.11Z) : 3 marchés CPV 03110000, 1 marché CPV 03120000 → total 4
        NAF B (02.22Z) : 2 marchés CPV 03110000 → total 2
        """
        captured = self._capture_output(monkeypatch)
        rows = (
            [(f"A{i}", "03110000", "01.11Z") for i in range(3)]  # NAF A / CPV 0311 ×3
            + [("A3", "03120000", "01.11Z")]  # NAF A / CPV 0312 ×1
            + [(f"B{i}", "03110000", "02.22Z") for i in range(2)]  # NAF B / CPV 0311 ×2
        )
        lf = pl.LazyFrame(
            {
                "uid": [r[0] for r in rows],
                "codeCPV": [r[1] for r in rows],
                "titulaire_activite_code": [r[2] for r in rows],
                "titulaire_activite_nomenclature": ["NAFREV2"] * len(rows),
                "donneesActuelles": [True] * len(rows),
            }
        )

        calculate_naf_cpv_matching(lf)
        df = captured["df"]

        def get(code, cpv):
            row = df.filter((pl.col("activite_code") == code) & (pl.col("cpv") == cpv))
            return row.to_dicts()[0]

        a_0311 = get("01.11Z", "03110000")
        assert a_0311["score"] == 0.75
        assert a_0311["rank"] == 1
        assert a_0311["nb_marches"] == 3

        a_0312 = get("01.11Z", "03120000")
        assert a_0312["score"] == 0.25
        assert a_0312["rank"] == 2
        assert a_0312["nb_marches"] == 1

        b_0311 = get("02.22Z", "03110000")
        assert b_0311["score"] == 1.0
        assert b_0311["rank"] == 1
        assert b_0311["nb_marches"] == 2

        # Colonnes et ordre exacts attendus en sortie
        assert df.columns == [
            "activite_nomenclature",
            "activite_code",
            "cpv",
            "score",
            "rank",
            "nb_marches",
        ]


class TestJoinPopulation:
    def test_join_population_via_siren(self):
        lf = pl.LazyFrame(
            {
                "acheteur_id": [
                    "21005400200012",  # Lyon
                    "21013055700019",  # Marseille
                    "12345678900012",  # inconnu
                    None,
                ],
                "uid": ["A", "B", "C", "D"],
            },
            schema={"acheteur_id": pl.Utf8, "uid": pl.Utf8},
        )

        csv_path = BASE_DIR / "tests/data/identifiants-communes-test.csv"
        result = join_population(lf, csv_path).collect().sort("uid")

        assert result["acheteur_population"].to_list() == [520000, 870000, None, None]

    def test_join_population_fichier_absent(self, tmp_path):
        lf = pl.LazyFrame({"acheteur_id": ["21005400200012"], "uid": ["A"]})
        result = join_population(lf, tmp_path / "inexistant.csv").collect()

        assert result["acheteur_population"].to_list() == [None]


class TestConsolidateAcrossDatasets:
    """Consolidation d'une version de marché à travers plusieurs datasets.

    Clé de version = (uid, dateNotification, codeCPV). Les champs scalaires sont
    coalescés (complétude puis récence) ; les titulaires distincts non-nuls sont
    conservés ; les lignes issues de >1 dataset sont estampillées decp_colmo.
    """

    @staticmethod
    def _lf(rows: list[dict]) -> pl.LazyFrame:
        """Construit un LazyFrame de test avec les colonnes du stade concaténation."""
        return (
            pl.DataFrame(
                rows,
                schema={
                    "uid": pl.Utf8,
                    "dateNotification": pl.Utf8,
                    "codeCPV": pl.Utf8,
                    "objet": pl.Utf8,
                    "montant": pl.Float64,
                    "titulaire_id": pl.Utf8,
                    "titulaire_typeIdentifiant": pl.Utf8,
                    "sourceDataset": pl.Utf8,
                    "sourceFile": pl.Utf8,
                    "datePublicationDonnees": pl.Utf8,
                },
            )
            .with_columns(
                pl.col("dateNotification").str.strptime(
                    pl.Date, "%Y-%m-%d", strict=False
                ),
                pl.col("datePublicationDonnees").str.strptime(
                    pl.Date, "%Y-%m-%d", strict=False
                ),
            )
            .lazy()
        )

    def test_merges_same_version_across_datasets(self):
        """Même (uid, dateNotification, codeCPV) dans 2 datasets, l'un sans titulaire :
        fusion en 1 version, titulaire non-nul conservé, objet de la ligne la plus
        complète, source = decp_colmo."""
        lf = self._lf(
            [
                # dataset pauvre : pas de titulaire
                {
                    "uid": "A",
                    "dateNotification": "2023-08-03",
                    "codeCPV": "386",
                    "objet": "23_2702 Fourniture",
                    "montant": 4_800_000.0,
                    "titulaire_id": None,
                    "titulaire_typeIdentifiant": None,
                    "sourceDataset": "pes_legacy",
                    "sourceFile": "url_pes",
                    "datePublicationDonnees": "2023-08-23",
                },
                # dataset riche : titulaire présent
                {
                    "uid": "A",
                    "dateNotification": "2023-08-03",
                    "codeCPV": "386",
                    "objet": "Fourniture",
                    "montant": 4_800_000.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "scrap",
                    "sourceFile": "url_scrap",
                    "datePublicationDonnees": "2023-08-23",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect()

        assert result.height == 1
        row = result.to_dicts()[0]
        assert row["titulaire_id"] == "S1"
        assert row["objet"] == "Fourniture"  # ligne la plus complète (avec titulaire)
        assert row["sourceDataset"] == DECP_COLMO_DATASET
        assert row["sourceFile"] == DECP_COLMO_RESOURCE_URL

    def test_preserves_distinct_contracts_same_uid(self):
        """Même uid + dateNotification mais codeCPV différents (collision d'id) :
        les deux versions sont conservées, source inchangée."""
        lf = self._lf(
            [
                {
                    "uid": "A",
                    "dateNotification": "2024-11-13",
                    "codeCPV": "45210000",
                    "objet": "travaux batiment",
                    "montant": 100.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2024-11-21",
                },
                {
                    "uid": "A",
                    "dateNotification": "2024-11-13",
                    "codeCPV": "45311200",
                    "objet": "electricite",
                    "montant": 200.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2024-11-21",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect().sort("codeCPV")

        assert result.height == 2
        assert result["codeCPV"].to_list() == ["45210000", "45311200"]
        assert result["objet"].to_list() == ["travaux batiment", "electricite"]
        assert result["sourceDataset"].to_list() == ["aws", "aws"]

    def test_preserves_cotraitants(self):
        """Une version avec 2 titulaires distincts (cotraitants) dans un seul dataset :
        les 2 lignes titulaire sont conservées, pas d'estampille decp_colmo."""
        lf = self._lf(
            [
                {
                    "uid": "A",
                    "dateNotification": "2024-11-13",
                    "codeCPV": "452",
                    "objet": "chantier",
                    "montant": 100.0,
                    "titulaire_id": "SPIE",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2024-11-21",
                },
                {
                    "uid": "A",
                    "dateNotification": "2024-11-13",
                    "codeCPV": "452",
                    "objet": "chantier",
                    "montant": 100.0,
                    "titulaire_id": "COMMINGES",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2024-11-21",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect().sort("titulaire_id")

        assert result.height == 2
        assert result["titulaire_id"].to_list() == ["COMMINGES", "SPIE"]
        assert result["sourceDataset"].to_list() == ["aws", "aws"]

    def test_single_dataset_unchanged(self):
        """Un marché présent dans un seul dataset n'est ni fusionné ni ré-estampillé."""
        lf = self._lf(
            [
                {
                    "uid": "A",
                    "dateNotification": "2024-01-01",
                    "codeCPV": "452",
                    "objet": "obj",
                    "montant": 100.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2024-01-05",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect()

        assert result.height == 1
        row = result.to_dicts()[0]
        assert row["sourceDataset"] == "aws"
        assert row["sourceFile"] == "url_aws"
        assert row["objet"] == "obj"

    def test_conflict_resolution_completeness_then_recency(self):
        """Conflit sur un champ scalaire non-nul : la ligne la plus complète gagne ;
        à complétude égale, la datePublicationDonnees la plus récente gagne."""
        lf = self._lf(
            [
                # Groupe X : complétude différente -> la plus complète gagne malgré une
                # datePublicationDonnees plus ancienne.
                {
                    "uid": "X",
                    "dateNotification": "2023-01-01",
                    "codeCPV": "111",
                    "objet": "X complet",
                    "montant": 100.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2023-01-01",
                },
                {
                    "uid": "X",
                    "dateNotification": "2023-01-01",
                    "codeCPV": "111",
                    "objet": "X pauvre",
                    "montant": None,
                    "titulaire_id": None,
                    "titulaire_typeIdentifiant": None,
                    "sourceDataset": "scrap",
                    "sourceFile": "url_scrap",
                    "datePublicationDonnees": "2023-06-01",
                },
                # Groupe Y : complétude égale -> la plus récente gagne.
                {
                    "uid": "Y",
                    "dateNotification": "2023-01-01",
                    "codeCPV": "222",
                    "objet": "Y ancien",
                    "montant": 10.0,
                    "titulaire_id": "S2",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2023-01-01",
                },
                {
                    "uid": "Y",
                    "dateNotification": "2023-01-01",
                    "codeCPV": "222",
                    "objet": "Y recent",
                    "montant": 10.0,
                    "titulaire_id": "S2",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "scrap",
                    "sourceFile": "url_scrap",
                    "datePublicationDonnees": "2023-06-01",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect().sort("uid")

        rows = {r["uid"]: r for r in result.to_dicts()}
        # X : complétude prime sur récence
        assert rows["X"]["objet"] == "X complet"
        assert rows["X"]["montant"] == 100.0
        # Y : à complétude égale, récence tranche
        assert rows["Y"]["objet"] == "Y recent"

    def test_intra_dataset_distinct_objets_not_merged(self):
        """Garde-fou anti perte de données : si un même dataset contient déjà plusieurs
        objets distincts pour la même (uid, dateNotification, codeCPV), il s'agit de
        contrats réellement distincts (collision d'id intra-dataset) — on ne fusionne
        pas, on conserve les deux versions."""
        lf = self._lf(
            [
                {
                    "uid": "A",
                    "dateNotification": "2025-11-17",
                    "codeCPV": "45210000",
                    "objet": "Consultation lots 9 12 16 21",
                    "montant": 100.0,
                    "titulaire_id": "S1",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2025-11-20",
                },
                {
                    "uid": "A",
                    "dateNotification": "2025-11-17",
                    "codeCPV": "45210000",
                    "objet": "Reconsultation lots 3 et 18",
                    "montant": 200.0,
                    "titulaire_id": "S2",
                    "titulaire_typeIdentifiant": "SIRET",
                    "sourceDataset": "aws",
                    "sourceFile": "url_aws",
                    "datePublicationDonnees": "2025-11-20",
                },
            ]
        )

        result = consolidate_across_datasets(lf).collect().sort("objet")

        assert result.height == 2
        assert result["objet"].to_list() == [
            "Consultation lots 9 12 16 21",
            "Reconsultation lots 3 et 18",
        ]
        assert result["sourceDataset"].to_list() == ["aws", "aws"]

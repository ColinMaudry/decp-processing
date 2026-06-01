import polars as pl
from polars.testing import assert_frame_equal

from src.tasks.transform import (
    apply_modifications,
    calculate_naf_cpv_matching,
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
                "activite_code": [None, None],
                "activite_nomenclature": [None, None],
                "donneesActuelles": [True, True],
            },
            schema_overrides={
                "activite_code": pl.String,
                "activite_nomenclature": pl.String,
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
                "activite_code": [None] * n + ["43.21B"] * n,
                "activite_nomenclature": [None] * n + ["NAFREV2"] * n,
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

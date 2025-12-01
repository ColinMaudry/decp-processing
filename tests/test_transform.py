import polars as pl
from polars.testing import assert_frame_equal

from tasks.transform import (
    prepare_etablissements,
    prepare_unites_legales,
    replace_with_modification_data,
)


class TestPrepareUnitesLegales:
    def test_prepare_unites_legales(self):
        lf = pl.LazyFrame(
            [
                # Cas 1: Personne morale$
                {
                    "siren": "111111111",
                    "denominationUniteLegale": "Org 1",
                    "prenomUsuelUniteLegale": None,
                    "nomUniteLegale": None,
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                },
                # Cas 2: Personne physique avec nom d'usage
                {
                    "siren": "222222222",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": "Zacroit",  # a la priorité
                    "statutDiffusionUniteLegale": "O",
                },
                # Cas 3: Personne physique sans nom d'usage
                {
                    "siren": "333333333",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                },
                # Cas 4: Nom non-diffusible
                {
                    "siren": "44444444",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "P",
                },
            ]
        )

        # Expected DataFrame
        expected_df = pl.DataFrame(
            [
                # Cas 1: denominationUniteLegale est préservé
                {"siren": "111111111", "denominationUniteLegale": "Org 1"},
                # Cas 2: denominationUniteLegale = prenom + nomUsage (Zacroit)
                {"siren": "222222222", "denominationUniteLegale": "Ambroise Zacroit"},
                # Cas 3: denominationUniteLegale = prenom + nom (Croizat)
                {"siren": "333333333", "denominationUniteLegale": "Ambroise Croizat"},
                # Cas 4: denominationUniteLegale = non-diffusible
                {
                    "siren": "44444444",
                    "denominationUniteLegale": "[Données personnelles non-diffusibles]",
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
                    "enseigne1Etablissement": "Dénom usuelle",
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
    def test_replace_with_modification_data(self):
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
                    "dateNotification": "2023-02-04",
                    "datePublicationDonnees": "2023-02-05",
                    "montant": 1500,
                    "dureeMois": 18,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "modification_id": 1,
                    "donneesActuelles": True,
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
                    "modification_id": 0,
                    "donneesActuelles": False,
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
                    "modification_id": 0,
                    "donneesActuelles": True,
                    "acheteur_id": "99999",
                },
            ]
        )

        # Call the function
        result_df = replace_with_modification_data(lf).collect()

        # print(
        #     expected_df["uid", "dateNotification", "montant", "modification_id"]
        #     .to_pandas()
        #     .to_string()
        # )

        # print(
        #     result_df["uid", "dateNotification", "montant", "modification_id"]
        #     .to_pandas()
        #     .to_string()
        # )
        # Assert the result matches the expected DataFrame
        assert_frame_equal(
            result_df, expected_df, check_column_order=False, check_dtypes=False
        )

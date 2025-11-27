import polars as pl
from polars.testing import assert_frame_equal

from tasks.transform import (
    replace_with_modification_data,
)


class TestHandleModificationsMarche:
    def test_handle_modifications_marche_all_cases(self):
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

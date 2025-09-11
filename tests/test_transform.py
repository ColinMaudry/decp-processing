import polars as pl
from polars.testing import assert_frame_equal

from tasks.transform import (
    remove_modifications_duplicates,
    remove_suffixes_from_uid_column,
    replace_with_modification_data,
)

# from abc import ABC, abstractmethod


class TestRemoveSuffixes:
    @staticmethod
    def _extract_uid_list(_df):
        return _df.sort("uid")["uid"].to_list()

    def test_no_suffixes(self):
        df = pl.LazyFrame(
            {
                "uid": [
                    "20240101",
                    "20240200",
                    "2024010103",
                    "2024010101",
                    "2025010108",
                ],
                "modifications": [[], [1], [1, 2], [], []],
            }
        )
        cleaned_df = remove_suffixes_from_uid_column(df)
        assert self._extract_uid_list(df.collect()) == self._extract_uid_list(
            cleaned_df.collect()
        )

    def test_suffixes(self):
        df = pl.LazyFrame(
            {
                "uid": [
                    "20240101",
                    "20240200",
                    "2024010103",
                    "2024010100",
                    "2025010102",
                    "202501010220",
                ],
                "modifications": [[1], [], [1] * 3, [], [1] * 2, [1] * 20],
            }
        )
        cleaned_df = remove_suffixes_from_uid_column(df)
        assert self._extract_uid_list(cleaned_df.collect()) == sorted(
            ["202401", "202402", "20240101", "20240101", "20250101", "2025010102"]
        )


class TestHandleModificationsMarche:
    def test_remove_modifications_duplicates(self):
        df = pl.LazyFrame(
            {
                "uid": ["202401", "20240101", "20240102", "20240102", "2025010203"],
                "modifications": [[], [1], [1, 2], [], []],
            }
        )

        cleaned_df = remove_modifications_duplicates(df).collect()
        assert len(cleaned_df) == 3
        assert cleaned_df.sort("uid")["uid"].to_list() == sorted(
            ["202401", "20240102", "2025010203"]
        )

    def test_handle_modifications_marche_all_cases(self):
        # Input LazyFrame
        lf = pl.LazyFrame(
            [
                {
                    "uid": 1,
                    "montant": 1000,
                    "dureeMois": 12,
                    "acheteur_id": "12345",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "modification_dateNotificationModification": "2023-01-02",
                    "modification_datePublicationDonneesModification": "2023-01-03",
                    "modification_montant": 1000,
                    "modification_dureeMois": 15,
                    "modification_titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00014"}},
                    ],
                },
                {
                    "uid": 1,
                    "montant": 1000,
                    "dureeMois": 12,
                    "acheteur_id": "12345",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "modification_dateNotificationModification": "2023-02-04",
                    "modification_datePublicationDonneesModification": "2023-02-05",
                    "modification_montant": 1500,
                    "modification_dureeMois": 18,
                    "modification_titulaires": None,
                },
                {
                    "uid": 2,
                    "montant": 2000,
                    "dureeMois": 24,
                    "acheteur_id": "88888",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}
                    ],
                    "dateNotification": "2023-02-02",
                    "datePublicationDonnees": "2023-02-03",
                    "modification_dateNotificationModification": "2023-02-03",
                    "modification_datePublicationDonneesModification": "2023-02-04",
                    "modification_montant": None,
                    "modification_dureeMois": 12,
                    "modification_titulaires": None,
                },
                {
                    "uid": 3,
                    "acheteur_id": "88888",
                    "montant": 10000,
                    "dureeMois": 36,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}
                    ],
                    "dateNotification": "2023-01-02",
                    "datePublicationDonnees": "2023-01-08",
                    "modification_dateNotificationModification": "2023-01-03",
                    "modification_datePublicationDonneesModification": "2023-01-12",
                    "modification_montant": 3000,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
                {
                    "uid": 4,
                    "acheteur_id": "77777",
                    "montant": 500,
                    "dureeMois": 10,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}
                    ],
                    "dateNotification": "2023-06-01",
                    "datePublicationDonnees": "2023-06-02",
                    "modification_dateNotificationModification": "2023-06-02",
                    "modification_datePublicationDonneesModification": "2023-06-03",
                    "modification_montant": None,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
                {
                    "uid": 4,
                    "montant": 500,
                    "dureeMois": 10,
                    "acheteur_id": "77777",
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}
                    ],
                    "dateNotification": "2023-06-01",
                    "datePublicationDonnees": "2023-06-02",
                    "modification_dateNotificationModification": "2023-06-03",
                    "modification_datePublicationDonneesModification": "2023-06-04",
                    "modification_montant": 1500,
                    "modification_dureeMois": None,
                    "modification_titulaires": None,
                },
            ]
        )

        # Expected DataFrame
        expected_df = pl.DataFrame(
            [
                {
                    "uid": 1,
                    "dateNotification": "2023-02-04",
                    "datePublicationDonnees": "2023-02-05",
                    "montant": 1500,
                    "dureeMois": 18,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00014"}},
                    ],
                    "modification_id": 2,
                    "donneesActuelles": True,
                    "acheteur_id": "12345",
                },
                {
                    "uid": 1,
                    "dateNotification": "2023-01-02",
                    "datePublicationDonnees": "2023-01-03",
                    "montant": 1000,
                    "dureeMois": 15,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00014"}},
                    ],
                    "modification_id": 1,
                    "donneesActuelles": False,
                    "acheteur_id": "12345",
                },
                {
                    "uid": 1,
                    "dateNotification": "2023-01-01",
                    "datePublicationDonnees": "2023-01-02",
                    "montant": 1000,
                    "dureeMois": 12,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    "modification_id": 0,
                    "donneesActuelles": False,
                    "acheteur_id": "12345",
                },
                {
                    "uid": 2,
                    "dateNotification": "2023-02-03",
                    "datePublicationDonnees": "2023-02-04",
                    "montant": 2000,
                    "dureeMois": 12,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}
                    ],
                    "modification_id": 1,
                    "donneesActuelles": True,
                    "acheteur_id": "88888",
                },
                {
                    "uid": 2,
                    "dateNotification": "2023-02-02",
                    "datePublicationDonnees": "2023-02-03",
                    "montant": 2000,
                    "dureeMois": 24,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}
                    ],
                    "modification_id": 0,
                    "donneesActuelles": False,
                    "acheteur_id": "88888",
                },
                {
                    "uid": 3,
                    "dateNotification": "2023-01-03",
                    "datePublicationDonnees": "2023-01-12",
                    "montant": 3000,
                    "dureeMois": 36,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}
                    ],
                    "modification_id": 1,
                    "donneesActuelles": True,
                    "acheteur_id": "88888",
                },
                {
                    "uid": 3,
                    "dateNotification": "2023-01-02",
                    "datePublicationDonnees": "2023-01-08",
                    "montant": 10000,
                    "dureeMois": 36,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}
                    ],
                    "modification_id": 0,
                    "donneesActuelles": False,
                    "acheteur_id": "88888",
                },
                {
                    "uid": 4,
                    "dateNotification": "2023-06-03",
                    "datePublicationDonnees": "2023-06-04",
                    "montant": 1500,
                    "dureeMois": 10,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}
                    ],
                    "modification_id": 2,
                    "donneesActuelles": True,
                    "acheteur_id": "77777",
                },
                {
                    "uid": 4,
                    "dateNotification": "2023-06-02",
                    "datePublicationDonnees": "2023-06-03",
                    "montant": 500,
                    "dureeMois": 10,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}
                    ],
                    "modification_id": 1,
                    "donneesActuelles": False,
                    "acheteur_id": "77777",
                },
                {
                    "uid": 4,
                    "dateNotification": "2023-06-01",
                    "datePublicationDonnees": "2023-06-02",
                    "montant": 500,
                    "dureeMois": 10,
                    "titulaires": [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}
                    ],
                    "modification_id": 0,
                    "donneesActuelles": False,
                    "acheteur_id": "77777",
                },
            ]
        )

        # Call the function
        result_df = replace_with_modification_data(lf).collect()

        print(
            expected_df["uid", "dateNotification", "montant", "modification_id"]
            .to_pandas()
            .to_string()
        )

        print(
            result_df["uid", "dateNotification", "montant", "modification_id"]
            .to_pandas()
            .to_string()
        )
        # Assert the result matches the expected DataFrame
        assert_frame_equal(
            result_df, expected_df, check_column_order=False, check_dtypes=False
        )

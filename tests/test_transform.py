from datetime import date

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
        # Input DataFrame
        df = pl.DataFrame(
            {
                "uid": [1, 2, 3, 4, 5],
                "modifications": [
                    # Multiples modifications
                    [
                        {
                            "modification": {
                                "id": 101,
                                "dateNotificationModification": date(2023, 1, 2),
                                "datePublicationDonneesModification": date(2023, 1, 3),
                                "montant": 1000,
                                "dureeMois": 15,
                                "titulaires": [
                                    {
                                        "titulaire": {
                                            "typeIdentifiant": "SIRET",
                                            "id": "00012",
                                        }
                                    },
                                    {
                                        "titulaire": {
                                            "typeIdentifiant": "SIRET",
                                            "id": "00013",
                                        }
                                    },
                                    {
                                        "titulaire": {
                                            "typeIdentifiant": "SIRET",
                                            "id": "00014",
                                        }
                                    },
                                ],
                            }
                        },
                        {
                            "modification": {
                                "id": 102,
                                "dateNotificationModification": date(2023, 1, 2),
                                "datePublicationDonneesModification": date(2023, 2, 2),
                                "montant": 1500,
                                "dureeMois": 18,
                            }
                        },
                    ],
                    # Modification sans montant
                    [
                        {
                            "modification": {
                                "id": 101,
                                "dateNotificationModification": date(2023, 2, 3),
                                "datePublicationDonneesModification": date(2023, 2, 4),
                                "dureeMois": 12,
                            }
                        }
                    ],
                    # Modification sans durée
                    [
                        {
                            "modification": {
                                "id": 101,
                                "dateNotificationModification": date(2023, 1, 10),
                                "datePublicationDonneesModification": date(2023, 1, 12),
                                "montant": 3000,
                            }
                        }
                    ],
                    # Multiples modifications dont une sans données
                    [
                        {
                            "modification": {
                                "id": 101,
                                "dateNotificationModification": date(2023, 6, 2),
                                "datePublicationDonneesModification": date(2023, 6, 3),
                            }
                        },
                        {
                            "modification": {
                                "id": 102,
                                "dateNotificationModification": date(2023, 6, 3),
                                "datePublicationDonneesModification": date(2023, 6, 4),
                                "montant": 1500,
                            }
                        },
                    ],
                    # Pas de modification
                    [None],
                ],
                "dateNotification": [
                    date(2023, 1, 1),
                    date(2023, 2, 2),
                    date(2023, 1, 2),
                    date(2023, 6, 1),
                    date(2024, 2, 10),
                ],
                "datePublicationDonnees": [
                    date(2023, 1, 2),
                    date(2023, 2, 3),
                    date(2023, 1, 8),
                    date(2023, 6, 2),
                    date(2024, 2, 12),
                ],
                "montant": [1000, 2000, 10000, 500, 5000],
                "dureeMois": [12, 24, 36, 10, 36],
                "titulaires": [
                    [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0005"}}],
                ],
            }
        )

        # Expected DataFrame
        expected_df = pl.DataFrame(
            {
                "uid": [1, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5],
                "modification_id": [2, 1, 0, 1, 0, 1, 0, 2, 1, 0, 0],
                "dateNotification": [
                    date(2023, 1, 2),
                    date(2023, 1, 2),
                    date(2023, 1, 1),
                    date(2023, 2, 3),
                    date(2023, 2, 2),
                    date(2023, 1, 10),
                    date(2023, 1, 2),
                    date(2023, 6, 3),
                    date(2023, 6, 2),
                    date(2023, 6, 1),
                    date(2024, 2, 10),
                ],
                "datePublicationDonnees": [
                    date(2023, 2, 2),
                    date(2023, 1, 3),
                    date(2023, 1, 2),
                    date(2023, 2, 4),
                    date(2023, 2, 3),
                    date(2023, 1, 12),
                    date(2023, 1, 8),
                    date(2023, 6, 4),
                    date(2023, 6, 3),
                    date(2023, 6, 2),
                    date(2024, 2, 12),
                ],
                "montant": [
                    1500,
                    1000,
                    1000,
                    2000,
                    2000,
                    3000,
                    10000,
                    1500,
                    500,
                    500,
                    5000,
                ],
                "dureeMois": [
                    18,
                    15,
                    12,
                    12,
                    24,
                    36,
                    36,
                    10,
                    10,
                    10,
                    36,
                ],
                "donneesActuelles": [
                    True,
                    False,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    False,
                    True,
                ],
                "titulaires": [
                    [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00014"}},
                    ],
                    [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00013"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00014"}},
                    ],
                    [
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00011"}},
                        {"titulaire": {"typeIdentifiant": "SIRET", "id": "00012"}},
                    ],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0002"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0003"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0004"}}],
                    [{"titulaire": {"typeIdentifiant": "SIRET", "id": "0005"}}],
                ],
            }
        )

        # Call the function
        result_df = replace_with_modification_data(df)

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

import pytest
import polars as pl
from tasks.transform import remove_modfications_duplicates, replace_by_modification_data
from polars.testing import assert_frame_equal
from datetime import date

class TestHandleModificationsMarche:
    def test_remove_modfications_duplicates(self):
        df = pl.LazyFrame({
            "uid": ["20240101-1", "20240101-2", "20240101-3", "20240101-4", "20250101-02"],
            "objet": ["TRUC", "TRUC", "TRUC", "TOTO", "TATA"],
            "montant": [100, 101, 150, 200, 500],
            "modifications": [[], [1], [1, 2], [], []]
        })

        cleaned_df = remove_modfications_duplicates(df).collect()
        assert len(cleaned_df) == 3
        assert cleaned_df.sort("uid")["uid"].to_list() == ["20240101", "20240101-4", "20250101-02"]

    def test_handle_modifications_marche_all_cases(self):
        # Input DataFrame
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "modifications": [
            # Multiples modifications
            [
                {"modification": {"id": 101, "dateNotificationModification": date(2023, 1, 2), "datePublicationDonneesModification": date(2023, 1, 3), "montant": 1000, "dureeMois": 15}},
                {"modification": {"id": 102, "dateNotificationModification": date(2023, 2, 1), "datePublicationDonneesModification": date(2023, 2, 2), "montant": 1500, "dureeMois": 18}}
            ],
            # Modification sans montant
            [{"modification": {"id": 101, "dateNotificationModification": date(2023, 2, 3), "datePublicationDonneesModification": date(2023, 2, 4), "dureeMois": 12}}],
            # Modification sans durée
            [{"modification": {"id": 101, "dateNotificationModification": date(2023, 1, 10), "datePublicationDonneesModification": date(2023, 1, 12), "montant": 3000}}],
            # Multiples modifications dont une sans données
            [
                 {"modification": {"id": 101, "dateNotificationModification": date(2023, 6, 2), "datePublicationDonneesModification": date(2023, 6, 3)}},
                 {"modification": {"id": 102, "dateNotificationModification": date(2023, 6, 3), "datePublicationDonneesModification": date(2023, 6, 4), "montant": 1500}}
            ],
            # Pas de modification
            [None]
            ],
            "dateNotification": [date(2023, 1, 1), date(2023, 2, 2), date(2023, 1, 2), date(2023, 6, 1), date(2024, 2, 10)],
            "datePublicationDonnees": [date(2023, 1, 2), date(2023, 2, 3), date(2023, 1, 8), date(2023, 6, 2), date(2024, 2, 12)],
            "montant": [1000, 2000, 10000, 500, 5000],
            "dureeMois": [12, 24, 36, 10, 36]
            })

        # Expected DataFrame
        expected_df = (
            pl.DataFrame({
            "id": [1, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5],
            "modification_id": [102, 101, 0, 101, 0, 101, 0, 102, 101, 0, 0],
            "dateNotification": [date(2023, 2, 1), date(2023, 1, 2), date(2023, 1, 1), date(2023, 2, 3), date(2023, 2, 2), date(2023, 1, 10), date(2023, 1, 2), date(2023, 6, 3), date(2023, 6, 2), date(2023, 6, 1), date(2024, 2, 10)],
            "datePublicationDonnees": [date(2023, 2, 2), date(2023, 1, 3), date(2023, 1, 2), date(2023, 2, 4), date(2023, 2, 3), date(2023, 1, 12), date(2023, 1, 8), date(2023, 6, 4), date(2023, 6, 3), date(2023, 6, 2), date(2024, 2, 12)],
            "montant": [1500, 1000, 1000, 2000, 2000, 3000, 10000, 1500, 500, 500, 5000],
            "dureeMois": [18, 15, 12, 12, 24, 36, 36, 10, 10, 10, 36],
            "estDerniereNotification": [True, False, False, True, False, True, False, True, False, False, True]
            })
        )

        # Call the function
        result_df = replace_by_modification_data(df)

        print(result_df)
        # Assert the result matches the expected DataFrame
        assert_frame_equal(result_df, expected_df)

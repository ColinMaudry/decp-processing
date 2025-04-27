import pytest
import polars as pl
from tasks.transform import handle_modifications_marche
from polars.testing import assert_frame_equal

class TestHandleModificationsMarche:
    def test_handle_modifications_marche_all_cases(self):
        # Input DataFrame
        df = pl.DataFrame({
                    "id": [1, 2, 3, 4, 5],
                    "modifications": [
                        # Multiples modifications
                        [
                            {"modification": {"id": 101, "dateNotificationModification": "2023-01-02", "datePublicationDonneesModification": "2023-01-03", "montant": 1000, "dureeMois": 15}},
                            {"modification": {"id": 102, "dateNotificationModification": "2023-02-01", "datePublicationDonneesModification": "2023-02-02", "montant": 1500, "dureeMois": 18}}
                        ],
                        # Modification sans montant
                        [{"modification": {"id": 101, "dateNotificationModification": "2023-02-03", "datePublicationDonneesModification": "2023-02-04", "dureeMois": 12}}],
                        # Modification sans durée
                        [{"modification": {"id": 101, "dateNotificationModification": "2023-01-10", "datePublicationDonneesModification": "2023-01-12", "montant": 3000}}],
                        # Multiples modifications dont une sans données
                        [
                             {"modification": {"id": 101, "dateNotificationModification": "2023-06-02", "datePublicationDonneesModification": "2023-06-03"}},
                             {"modification": {"id": 102, "dateNotificationModification": "2023-06-03", "datePublicationDonneesModification": "2023-06-04", "montant": 1500}}
                        ],
                        # Pas de modification
                        [None]
                    ],
                    "dateNotification": ["2023-01-01", "2023-02-02", "2023-01-02", "2023-06-01", "2024-02-10"],
                    "datePublicationDonnees": ["2023-01-02", "2023-02-03", "2023-01-08", "2023-06-02", "2024-02-12"],
                    "montant": [1000, 2000, 10000, 500, 5000],
                    "dureeMois": [12, 24, 36, 10, 36]
                })

        # Expected DataFrame
        expected_df = (
            pl.DataFrame({
                "id": [1, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5],
                "modification_id": [102, 101, 0, 101, 0, 101, 0, 102, 101, 0, 0],
                "dateNotification": ["2023-02-01", "2023-01-02", "2023-01-01", "2023-02-03", "2023-02-02", "2023-01-10", "2023-01-02", "2023-06-03", "2023-06-02", "2023-06-01", "2024-02-10"],
                "datePublicationDonnees": ["2023-02-02", "2023-01-03", "2023-01-02", "2023-02-04", "2023-02-03", "2023-01-12", "2023-01-08", "2023-06-04", "2023-06-03", "2023-06-02", "2024-02-12"],
                "montant": [1500, 1000, 1000, 2000, 2000, 3000, 10000, 1500, 500, 500, 5000],
                "dureeMois": [18, 15, 12, 12, 24, 36, 36, 10, 10, 10, 36],
                "estDerniereNotification": [True, False, False, True, False, True, False, True, False, False, True]
            })
            .with_columns(pl.col("datePublicationDonnees").str.to_datetime(format="%Y-%m-%d"))
            .with_columns(pl.col("dateNotification").str.to_datetime(format="%Y-%m-%d"))
            )

        # Call the function
        result_df = handle_modifications_marche(df)

        print(result_df)
        # Assert the result matches the expected DataFrame
        assert_frame_equal(result_df, expected_df)

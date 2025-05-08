from src.tasks.transform import (
    remove_modfications_duplicates,
    replace_by_modification_data,
)
import polars as pl


def test_remove_modfications_duplicates():
    df = pl.LazyFrame(
        {
            "uid": [
                "20240101-1",
                "20240101-2",
                "20240101-3",
                "20240101-4",
                "20250101-02",
            ],
            "objet": ["TRUC", "TRUC", "TRUC", "TOTO", "TATA"],
            "montant": [100, 101, 150, 200, 500],
            "modifications": [[], [1], [1, 2], [], []],
        }
    )

    cleaned_df = remove_modfications_duplicates(df).collect()
    assert len(cleaned_df) == 3
    assert cleaned_df.sort("uid")["uid"].to_list() == [
        "20240101",
        "20240101-4",
        "20250101-02",
    ]


def test_replace_by_modification_data():
    df = pl.LazyFrame(
        {
            "modif_id": ["20240101-3", "20250101-4", "20250102-1"],
            "id": ["20240101-3", "20250101-4", "20250102-1"],
            "objet": ["TRUC", "TATA", "TOTO"],
            "montant": [150, 500, 200],
            "dureeMois": [1, 2, 3],
            "modifications": [
                [
                    {
                        "modification": {
                            "id": 1,
                            "dateNotificationModification": "2023-06-16",
                            "datePublicationDonneesModification": "2024-05-24",
                            "dureeMois": None,
                            "montant": 151.0,
                            "titulaires": None,
                        }
                    },
                    {
                        "modification": {
                            "id": 1,
                            "dateNotificationModification": "2023-06-16",
                            "datePublicationDonneesModification": "2024-05-24",
                            "dureeMois": None,
                            "montant": 152.0,
                            "titulaires": None,
                        }
                    },
                    {
                        "modification": {
                            "id": 1,
                            "dateNotificationModification": "2023-06-16",
                            "datePublicationDonneesModification": "2024-05-24",
                            "dureeMois": 2,
                            "montant": None,
                            "titulaires": None,
                        }
                    },
                ],
                [
                    {
                        "modification": {
                            "id": 1,
                            "dateNotificationModification": "2023-06-16",
                            "datePublicationDonneesModification": "2024-05-24",
                            "dureeMois": None,
                            "montant": 505,
                            "titulaires": None,
                        }
                    }
                ],
                [],
            ],
        }
    )

    modified_df = replace_by_modification_data(df).sort("modif_id").collect()
    assert len(modified_df) == 3
    assert modified_df["montant"].to_list() == [152.0, 505.0, 200.0]
    assert modified_df["dureeMois"].to_list() == [2, 2, 3]

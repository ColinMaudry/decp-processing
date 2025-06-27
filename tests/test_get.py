from unittest import TestCase

from tasks.get import clean_json


class TestGet:
    def test_clean_json(self):
        input_json = [
            {
                "id": "2020-698182-01",
                "acheteur": {"id": "22870851700989"},
                "dureeMois": 40,
                "dateNotification": "2020-02-06",
                "datePublicationDonnees": "2020-02-14",
                "montant": 23004.0,
                "titulaires": [
                    {"titulaire": {"typeIdentifiant": "SIRET", "id": "83415751300245"}}
                ],
                "modifications": [
                    {
                        "modification": {
                            "dateNotificationModification": "2023-06-07",
                            "dureeMois": 1,
                            "montant": 1,
                            "id": 1,
                            "titulaires": [
                                {"titulaire": "typeIdentifiant"},
                                {"titulaire": "id"},
                            ],
                            "datePublicationDonneesModification": "2023-06-20",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2025-04-01",
                            "dureeMois": 1,
                            "montant": 22545,
                            "id": 2,
                            "datePublicationDonneesModification": "2025-04-01",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2025-04-01",
                            "dureeMois": 2,
                            "montant": 22540,
                            "id": 2,
                            "titulaires": [
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "58211867500054",
                                    }
                                },
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "05650171100115",
                                    }
                                },
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "88359829400030",
                                    }
                                },
                            ],
                            "datePublicationDonneesModification": "2025-04-01",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2027-04-01",
                            "dureeMois": 100,
                            "id": 3,
                            "datePublicationDonneesModification": "2027-04-01",
                        }
                    },
                ],
                "origineFrance": 0.0,
            }
        ]

        # Expected DataFrame
        expected_json = [
            {
                "id": "2020-698182-01",
                "acheteur": {"id": "22870851700989"},
                "dureeMois": 40,
                "dateNotification": "2020-02-06",
                "datePublicationDonnees": "2020-02-14",
                "montant": 23004.0,
                "titulaires": [
                    {"titulaire": {"typeIdentifiant": "SIRET", "id": "83415751300245"}}
                ],
                "modifications": [
                    {
                        "modification": {
                            "dateNotificationModification": "2023-06-07",
                            "dureeMois": 1,
                            "montant": 1,
                            "id": 1,
                            "datePublicationDonneesModification": "2023-06-20",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2025-04-01",
                            "dureeMois": 1,
                            "montant": 22545,
                            "id": 2,
                            "datePublicationDonneesModification": "2025-04-01",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2025-04-01",
                            "dureeMois": 2,
                            "montant": 22540,
                            "id": 2,
                            "titulaires": [
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "58211867500054",
                                    }
                                },
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "05650171100115",
                                    }
                                },
                                {
                                    "titulaire": {
                                        "typeIdentifiant": "SIRET",
                                        "id": "88359829400030",
                                    }
                                },
                            ],
                            "datePublicationDonneesModification": "2025-04-01",
                        }
                    },
                    {
                        "modification": {
                            "dateNotificationModification": "2027-04-01",
                            "dureeMois": 100,
                            "id": 3,
                            "datePublicationDonneesModification": "2027-04-01",
                        }
                    },
                ],
                "origineFrance": 0.0,
            }
        ]

        # Call the function
        result_json = clean_json(input_json)
        print("Result JSON:", result_json)
        print("Expected JSON:", expected_json)
        # Assert the result matches the expected DataFrame
        for result, expected in zip(result_json, expected_json):
            TestCase().assertDictEqual(expected, result)

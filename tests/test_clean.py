import datetime

import polars as pl

from src.config import DecpFormat
from src.schemas import SCHEMA_MARCHE_2019, SCHEMA_MARCHE_2022
from src.tasks.clean import (
    clean_decp,
    clean_invalid_characters,
    clean_null_equivalent,
    clean_titulaires,
    extract_innermost_struct,
    fix_data_types,
    relire_dates_mal_converties,
)


def test_extract_innermost_struct():
    # Test case 1: Simple dictionary - returns None as it expects list
    data = {"a": 1}
    assert extract_innermost_struct(data) is None

    # Test case 2: Nested list with dictionary - returns the list containing the dict
    data = [[{"a": 1}]]
    assert extract_innermost_struct(data) == [{"a": 1}]

    # Test case 3: Deeply nested list
    data = [[[[{"a": 1}]]]]
    assert extract_innermost_struct(data) == [{"a": 1}]

    # Test case 4: Empty list
    data = []
    assert extract_innermost_struct(data) == []

    # Test case 5: List with empty list
    data = [[]]
    assert extract_innermost_struct(data) == []

    # Test case 6: Fallback (not a list)
    data = "string"
    assert extract_innermost_struct(data) is None


def test_clean_invalid_characters():
    # Test case 1: String with no invalid characters
    data = b"Hello World"
    assert clean_invalid_characters(data) == data

    # Test case 2: String with invalid characters (e.g., null byte)
    data = b"Hello\x00World"
    assert clean_invalid_characters(data) == b"HelloWorld"

    # Test case 3: String with other control characters
    data = b"Hello\x01World"
    assert clean_invalid_characters(data) == b"HelloWorld"


def test_clean_null_equivalent():
    # Setup data
    data = {
        "considerationsSociales": ["Pas de considération sociale", "Oui", "Non"],
        "considerationsEnvironnementales": [
            "Pas de considération environnementale",
            "Oui",
            "Non",
        ],
        "ccag": ["Pas de CCAG", "CCAG Travaux", "Autre"],
        "typeGroupement": ["Pas de groupement", "Conjoint", "Solidaire"],
        "other_col": ["A", "B", "C"],
    }
    lf = pl.LazyFrame(data)

    # Execute function
    result = clean_null_equivalent(lf).collect()

    # Assertions
    assert result["considerationsSociales"][0] == "Sans objet"
    assert result["considerationsSociales"][1] == "Oui"
    assert result["considerationsEnvironnementales"][0] == "Sans objet"
    assert result["ccag"][0] == "Sans objet"
    assert result["typeGroupement"][0] == "Sans objet"
    assert result["other_col"][0] == "A"


def test_clean_titulaires():
    decp_format_2019 = DecpFormat("DECP 2019", SCHEMA_MARCHE_2019, "marches")
    decp_format_2022 = DecpFormat("DECP 2022", SCHEMA_MARCHE_2022, "marches.marche")

    # Test DECP 2019 format
    data_2019 = {
        "titulaires": [
            [
                {"id": "1", "typeIdentifiant": "SIRET"},
                {"id": "2", "typeIdentifiant": "SIRET"},
            ],
            [{"id": None, "typeIdentifiant": None}],  # Should be filtered out
            [],  # Empty list
        ],
        "modification_titulaires": [[], [], []],
    }
    schema_2019 = {
        "titulaires": pl.List(pl.Struct({"id": pl.Utf8, "typeIdentifiant": pl.Utf8})),
        "modification_titulaires": pl.List(
            pl.Struct({"id": pl.Utf8, "typeIdentifiant": pl.Utf8})
        ),
    }
    lf_2019 = pl.LazyFrame(data_2019, schema=schema_2019)
    result_2019 = clean_titulaires(lf_2019, decp_format_2019, "titulaires").collect()

    titulaires_0 = result_2019["titulaires"][0]
    assert len(titulaires_0) == 2
    assert titulaires_0[0]["titulaire_id"] == "1"
    assert titulaires_0[0]["titulaire_typeIdentifiant"] == "SIRET"

    # Check that empty/null titulaires are handled (filtered out or result is null if empty list)
    # The code filters elements where id AND typeIdentifiant are null.
    # And then replaces empty lists with None.

    # Row 1: [{"id": None, "typeIdentifiant": None}] -> [] -> None
    assert result_2019["titulaires"][1] is None

    # Row 2: [] -> [] -> None
    assert result_2019["titulaires"][2] is None

    # Test DECP 2022 format
    data_2022 = {
        "titulaires": [
            [{"titulaire": {"id": "3", "typeIdentifiant": "SIRET"}}],
        ],
        "modification_titulaires": [[]],
    }
    schema_2022 = {
        "titulaires": pl.List(
            pl.Struct(
                {"titulaire": pl.Struct({"id": pl.Utf8, "typeIdentifiant": pl.Utf8})}
            )
        ),
        "modification_titulaires": pl.List(
            pl.Struct(
                {"titulaire": pl.Struct({"id": pl.Utf8, "typeIdentifiant": pl.Utf8})}
            )
        ),
    }
    lf_2022 = pl.LazyFrame(data_2022, schema=schema_2022)
    result_2022 = clean_titulaires(lf_2022, decp_format_2022, "titulaires").collect()

    titulaires_0 = result_2022["titulaires"][0]
    assert len(titulaires_0) == 1
    assert titulaires_0[0]["titulaire_id"] == "3"


def test_fix_data_types():
    data = {
        "dureeMois": ["12", "invalid", None, None, None],
        "montant": ["100.50", "invalid", "200", "300", "400"],
        "dateNotification": [
            "2023-01-01",
            "invalid-date",
            "2023-12-31",
            "2023-01-02",
            "2023-01-03",
        ],
        "sousTraitanceDeclaree": ["true", "false", "Oui", "Non", "invalid"],
        "marcheInnovant": ["1", "0", "invalid", "0", "1"],
        "attributionAvance": ["true", "false", "invalid", "false", "true"],
        "offresRecues": ["1", "2", "3", "4", "5"],
        "tauxAvance": ["0", "0", "0", "0", "0"],
        "origineFrance": ["0", "0", "0", "0", "0"],
        "origineUE": ["0", "0", "0", "0", "0"],
        "modification_id": ["0", "0", "0", "0", "0"],
        "datePublicationDonnees": [
            "2023-01-01",
            "2023-01-01",
            "2023-01-01",
            "2023-01-01",
            "2023-01-01",
        ],
    }
    lf = pl.LazyFrame(data)
    result = fix_data_types(lf).collect()

    # Check numeric types
    assert result["dureeMois"].dtype == pl.Int16
    assert result["dureeMois"][0] == 12
    assert result["dureeMois"][1] is None

    assert result["montant"].dtype == pl.Float64
    assert result["montant"][0] == 100.50
    assert result["montant"][1] is None

    # Check dates
    assert result["dateNotification"].dtype == pl.Date
    assert result["dateNotification"][0] == datetime.date(2023, 1, 1)
    assert result["dateNotification"][1] is None

    # Check booleans
    assert result["sousTraitanceDeclaree"].dtype == pl.Boolean
    assert result["sousTraitanceDeclaree"][0] is True
    assert result["sousTraitanceDeclaree"][1] is False
    assert result["sousTraitanceDeclaree"][2] is True
    assert result["sousTraitanceDeclaree"][3] is False
    assert result["sousTraitanceDeclaree"][4] is None

    assert result["marcheInnovant"].dtype == pl.Boolean
    assert result["marcheInnovant"][0] is True
    assert result["marcheInnovant"][1] is False


def test_clean_decp():
    # Minimal test for clean_decp to ensure the pipeline runs

    # clean_decp expects certain columns
    data = {
        "id": ["id.1", "id/2", ""],
        "acheteur_id": ["ach1", "ach2", ""],
        "acheteur.id": ["", "ach2", ""],
        "objet": "Avec des 'apo'strophe's",
        "montant": ["1000", "1000000000000.00", "2000"],
        "datePublicationDonnees": ["2023-01-01", "0002-11-30", "2023-01-02"],
        "dateNotification": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "nature": ["Marche subsequent", "", "Autre"],
        "codeCPV": ["123456780-1", "876541", "11111111111"],
        "titulaires": [
            [{"id": "t1", "typeIdentifiant": "SIRET"}],
            [{"id": "t2", "typeIdentifiant": "SIRET"}],
            [],
        ],
        # On ne teste pas les modifications
        "modification_titulaires": [None, None, None],
        "modification_id": [None, None, None],
        # Columns for string lists
        "considerationsSociales_considerationSociale": [
            ["Clause sociale", "Critère social"],
            ["Critère social"],
            [""],
        ],
        "considerationsSociales": [[], [], []],
        # Columns for fix_data_types
        "dureeMois": ["12", "24", "36"],
        "offresRecues": ["1", "2", "3"],
        "tauxAvance": ["0", "0", "0"],
        "origineFrance": ["0", "0", "0"],
        "origineUE": ["0", "0", "0"],
        "sousTraitanceDeclaree": ["false", "false", "false"],
        "attributionAvance": ["false", "false", "false"],
        "marcheInnovant": ["false", "false", "false"],
        # Columns for clean_null_equivalent
        "considerationsEnvironnementales": [
            ["Pas de considération environnementale"],
            ["Clause environnementale"],
            [""],
        ],
        "ccag": ["Pas de CCAG", "CCAG", ""],
        "typeGroupement": ["Pas de groupement", "Solidaire", ""],
    }

    lf = pl.LazyFrame(data)

    # Test with DECP 2019
    decp_format_2019 = DecpFormat("DECP 2019", SCHEMA_MARCHE_2019, "marches")
    df_result: pl.DataFrame = clean_decp(lf, decp_format_2019).sort("id").collect()

    # Check id cleaning
    assert df_result.filter(pl.col("id") == "id_1").height > 0
    assert df_result.filter(pl.col("id") == "id_2").height > 0

    # Empty id/acheteur_id should be filtered out
    assert df_result.filter(pl.col("id") == "").height == 0

    # Check uid creation : acheteur_id + id + "_" + codeCPV (nettoyé) pour distinguer
    # les contrats distincts partageant le même (acheteur_id, id) — cf. issue #186
    assert df_result["uid"].to_list() == [
        "ach1id_1_12345678",
        "ach2id_2_87654100",
    ]

    # Check montant replacement
    assert 12311111111.0 in df_result["montant"].to_list()
    assert "1.0E17" not in df_result["montant"].to_list()

    # Check string lists
    assert "considerationsSociales_considerationSociale" not in df_result
    assert "considerationsSociales" in df_result
    assert df_result["considerationsSociales"].to_list() == [
        "Clause sociale, Critère social",
        "Critère social",
    ]

    # Check null equivalent
    assert df_result["considerationsEnvironnementales"][0] == "Sans objet"
    assert df_result["ccag"][0] == "Sans objet"
    assert df_result["typeGroupement"][0] == "Sans objet"
    assert df_result["objet"][0] == "Avec des 'apo’strophe’s"
    assert df_result["objet"][0] == "Avec des 'apo’strophe’s"

    # Check nature replacement
    assert df_result["nature"][0] == "Marché subséquent"

    # Check codeCPV
    assert df_result["codeCPV"].to_list() == ["12345678", "87654100"]


class TestRelireDatesMalConverties:
    """Deux formats source produisent le même symptôme, cf. issue #191."""

    @staticmethod
    def _lf(notification, publication, identifiants):
        return pl.LazyFrame(
            {
                "id": identifiants,
                "dateNotification": notification,
                "datePublicationDonnees": publication,
            },
            schema={
                "id": pl.String,
                "dateNotification": pl.String,
                "datePublicationDonnees": pl.String,
            },
        )

    def _relire(self, notification, publication, identifiant=None):
        lf = self._lf([notification], [publication], [identifiant])
        return relire_dates_mal_converties(lf).collect()["dateNotification"][0]

    def test_jjmmaa_tranche_par_la_publication(self):
        # 0031-05-22 : JJ-MM-AA donne 2022-05-31, AA-MM-JJ donnerait 2031-05-22,
        # postérieur à la publication.
        assert self._relire("0031-05-22", "2022-06-15") == "2022-05-31"

    def test_aammjj_tranche_par_la_publication(self):
        # 0022-05-13 : AA-MM-JJ donne 2022-05-13, JJ-MM-AA donnerait 2013-05-22,
        # neuf ans avant la publication.
        assert self._relire("0022-05-13", "2022-06-15") == "2022-05-13"

    def test_millesime_tranche_sans_publication(self):
        assert self._relire("0031-05-22", None, "2022V2207201") == "2022-05-31"

    def test_deux_lectures_plausibles_ne_sont_pas_tranchees(self):
        # JJ-MM-AA donne 2025-06-24, AA-MM-JJ donne 2024-06-25 : les deux précèdent
        # la publication et aucun millésime ne départage.
        assert self._relire("0024-06-25", "2026-01-10") == "0024-06-25"

    def test_millesime_trop_eloigne_ne_tranche_pas(self):
        assert self._relire("0031-05-22", None, "2010ABC01") == "0031-05-22"

    def test_millesime_equidistant_ne_tranche_pas(self):
        # JJ-MM-AA donne 2024, AA-MM-JJ donne 2026, tous deux à un an de 2025.
        assert self._relire("0026-06-24", None, "2025ABC01") == "0026-06-24"

    def test_le_millesime_ne_retient_pas_ce_que_la_publication_dement(self):
        # Le millésime 2024 désignerait 2024-06-26, postérieur à la publication.
        assert self._relire("0026-06-24", "2023-01-01", "2024ABC01") == "0026-06-24"

    def test_relecture_dans_le_futur_refusee(self):
        futur = datetime.date.today().year + 1
        annee_courte = str(futur - 2000).zfill(2)
        assert self._relire(f"0031-05-{annee_courte}", None, f"{futur}ABC01") == (
            f"0031-05-{annee_courte}"
        )

    def test_dates_valides_et_nulles_intactes(self):
        assert self._relire("2022-05-31", "2022-06-15") == "2022-05-31"
        assert self._relire(None, "2022-06-15") is None
        assert self._relire("pas-une-date", "2022-06-15") == "pas-une-date"

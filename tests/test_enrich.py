import polars as pl
from polars.testing import assert_frame_equal

from src.config import BASE_DIR
from src.tasks.enrich import add_etablissement_data, add_unite_legale_data


class TestEnrich:
    def test_add_unites_legales_data_titulaires(self):
        lf_sirets = pl.LazyFrame({"titulaire_id": ["12345678900022", "12345679000023"]})

        lf_unites_legales = pl.scan_parquet(
            BASE_DIR / "tests/data/sirene/unites_legales.parquet"
        )

        lf_output = pl.LazyFrame(
            {
                "titulaire_id": ["12345678900022", "12345679000023"],
                "titulaire_nom": ["Org 1", "Org 2"],
                "titulaire_siren": ["123456789", "123456790"],
                "titulaire_categorie": ["ETI", None],
            }
        )

        assert_frame_equal(
            add_unite_legale_data(
                lf_sirets, lf_unites_legales, "titulaire_id", "titulaire"
            ).collect(),
            lf_output.collect(),
            check_column_order=False,
        )

    def test_add_unites_legales_data_acheteurs(self):
        lf_sirets = pl.LazyFrame({"acheteur_id": ["12345678900022", "12345679000023"]})

        lf_unites_legales = pl.scan_parquet(
            BASE_DIR / "tests/data/sirene/unites_legales.parquet"
        )

        lf_output = pl.LazyFrame(
            {
                "acheteur_id": ["12345678900022", "12345679000023"],
                "acheteur_nom": ["Org 1", "Org 2"],
                "acheteur_siren": ["123456789", "123456790"],
            }
        )

        assert_frame_equal(
            add_unite_legale_data(
                lf_sirets, lf_unites_legales, "acheteur_id", "acheteur"
            ).collect(),
            lf_output.collect(),
            check_column_order=False,
        )

    def test_add_etablissement_data(self):
        lf_sirets = pl.LazyFrame(
            {"org_id": ["12345678900022", "12345678900023"], "org_nom": ["Org", "Org"]}
        )

        lf_etablissement = pl.scan_parquet(
            BASE_DIR / "tests/data/sirene/etablissements.parquet"
        )

        lf_output = pl.LazyFrame(
            {
                "org_id": ["12345678900022", "12345678900023"],
                "org_latitude": [11.12, 11.12],
                "org_longitude": [12.13, 12.13],
                "org_commune_code": ["12345", "12345"],
                "org_departement_code": ["12", "12"],
                "org_region_code": ["01", "01"],
                "org_commune_nom": ["Commune", "Commune"],
                "org_departement_nom": ["Département", "Département"],
                "org_region_nom": ["Région", "Région"],
                "org_nom": ["Org (Établissement nom)", "Org"],
                "activite_code": ["11.11A", "11.11B"],
                "activite_nomenclature": ["NAFRev2", "NAFRev2"],
            }
        )

        assert_frame_equal(
            add_etablissement_data(
                lf_sirets, lf_etablissement, "org_id", "org"
            ).collect(),
            lf_output.collect(),
            check_column_order=False,
        )

import polars as pl
from polars.testing import assert_frame_equal

from src.tasks.enrich import add_etablissement_data


class TestEnrich:
    def test_add_etablissement_data(self):
        lf_sirets = pl.LazyFrame({"org_id": ["12345678900022"]})

        lf_etablissement = pl.LazyFrame(
            {
                "siret": ["12345678900022"],
                "latitude": [11.12],
                "longitude": [12.13],
                "commune_code": ["12345"],
                "departement_code": ["12"],
                "region_code": ["01"],
                "commune_nom": ["Commune"],
                "departement_nom": ["Département"],
                "region_nom": ["Région"],
            }
        )

        lf_output = pl.LazyFrame(
            {
                "org_id": ["12345678900022"],
                "org_latitude": [11.12],
                "org_longitude": [12.13],
                "org_commune_code": ["12345"],
                "org_departement_code": ["12"],
                "org_region_code": ["01"],
                "org_commune_nom": ["Commune"],
                "org_departement_nom": ["Département"],
                "org_region_nom": ["Région"],
            }
        )

        assert_frame_equal(
            add_etablissement_data(
                lf_sirets, lf_etablissement, "org_id", "org"
            ).collect(),
            lf_output.collect(),
        )

import polars as pl
from polars.testing import assert_frame_equal

from src.config import SIRENE_DATA_DIR
from src.tasks.enrich import add_etablissement_data


class TestEnrich:
    def test_add_etablissement_data(self):
        lf_sirets = pl.LazyFrame(
            {"org_id": ["12345678900022", "12345678900023"], "org_nom": ["Org", "Org"]}
        )

        lf_etablissement = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")

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

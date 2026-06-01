import json
from datetime import date, timedelta

import httpx
import polars as pl
from polars.testing import assert_frame_equal

from src.config import BASE_DIR
from src.tasks.enrich import (
    add_etablissement_data,
    add_type_marche,
    add_unite_legale_data,
    geocode_missing_sirets,
    select_sirets_to_geocode,
)


class TestEnrich:
    def test_add_unites_legales_data_titulaires(self):
        lf_sirets = pl.LazyFrame({"titulaire_id": ["12345678900022", "12345679000023"]})

        lf_unites_legales = pl.DataFrame(
            json.load(open(BASE_DIR / "tests/data/sirene/unites_legales.json", "r"))
        ).lazy()

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
        lf_sirets = pl.LazyFrame(
            {"acheteur_id": ["12345678900022", "12345679000023", "20007695800011"]}
        )

        lf_unites_legales = pl.DataFrame(
            json.load(open(BASE_DIR / "tests/data/sirene/unites_legales.json", "r"))
        ).lazy()

        lf_output = pl.LazyFrame(
            {
                "acheteur_id": ["12345678900022", "12345679000023", "20007695800011"],
                "acheteur_nom": ["Org 1", "Org 2", "Collectivité de Corse"],
                "acheteur_siren": ["123456789", "123456790", "200076958"],
                "acheteur_categorie": [None, "Région", "Région"],
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

        lf_etablissements = pl.DataFrame(
            json.load(open(BASE_DIR / "tests/data/sirene/etablissements.json", "r"))
        ).lazy()

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
                lf_sirets, lf_etablissements, "org_id", "org"
            ).collect(),
            lf_output.collect(),
            check_column_order=False,
        )

    def test_select_sirets_excludes_success_and_recent_failures_and_not_in_sirene(self):
        today = date(2026, 5, 15)
        retry_days = 30

        lf_decp = pl.LazyFrame(
            {
                "acheteur_id": ["11111111111111", "22222222222222", "44444444444444"],
                "titulaire_id": ["33333333333333", "22222222222222", "55555555555555"],
            }
        )
        lf_siret_latlong = pl.LazyFrame(
            {
                "siret": [
                    "11111111111111",  # success → exclu
                    "22222222222222",  # failed récent (5j) → exclu
                    "33333333333333",  # failed ancien (60j) → INCLUS (à retenter)
                    "44444444444444",  # not_in_sirene → exclu
                ],
                "latitude": [48.0, None, None, None],
                "longitude": [2.0, None, None, None],
                "source": [
                    "decp",
                    "geoplateforme",
                    "geoplateforme",
                    "geoplateforme",
                ],
                "score": [None, 0.2, 0.1, None],
                "geocoded_at": [
                    None,
                    today - timedelta(days=5),
                    today - timedelta(days=60),
                    today - timedelta(days=10),
                ],
                "status": ["success", "failed", "failed", "not_in_sirene"],
            },
            schema_overrides={"geocoded_at": pl.Date},
        )

        result = select_sirets_to_geocode(
            lf_decp, lf_siret_latlong, today, retry_days
        ).collect()
        sirets = set(result["siret"].to_list())
        assert sirets == {"33333333333333", "55555555555555"}

    def test_geocode_missing_sirets_appends_new_entries_and_marks_not_in_sirene(
        self, monkeypatch
    ):
        today = date(2026, 5, 15)

        lf_decp = pl.LazyFrame(
            {
                "acheteur_id": ["11111111111111", "99999999999999"],
                "titulaire_id": ["22222222222222", "99999999999999"],
            }
        )
        lf_siret_latlong = pl.LazyFrame(
            {
                "siret": ["00000000000000"],
                "latitude": [48.0],
                "longitude": [2.0],
                "source": ["decp"],
                "score": [None],
                "geocoded_at": [None],
                "status": ["success"],
            },
            schema_overrides={"geocoded_at": pl.Date, "score": pl.Float64},
        )
        lf_etab = pl.LazyFrame(
            {
                "siret": ["11111111111111", "22222222222222"],
                "numeroVoieEtablissement": ["10", "5"],
                "indiceRepetitionEtablissement": [None, None],
                "typeVoieEtablissement": ["RUE", "AVENUE"],
                "libelleVoieEtablissement": ["DE LA PAIX", "DE LYON"],
                "codePostalEtablissement": ["75001", "69001"],
                "commune_code": ["75101", "69381"],
            }
        )

        _original_Client = httpx.Client

        def handler(request: httpx.Request) -> httpx.Response:
            body = (
                "siret,q,postcode,citycode,result_score,latitude,longitude\n"
                "11111111111111,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
                "22222222222222,5 AVENUE DE LYON,69001,69381,0.3,45.7,4.8\n"
            )
            return httpx.Response(200, content=body)

        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            "src.tasks.geocode.httpx.Client",
            lambda **kw: _original_Client(
                transport=transport,
                **{k: v for k, v in kw.items() if k != "transport"},
            ),
        )
        monkeypatch.setattr(
            "src.tasks.enrich.date",
            _FrozenDate(today),
        )

        updated = geocode_missing_sirets(lf_decp, lf_siret_latlong, lf_etab).collect()
        by_siret = {row["siret"]: row for row in updated.iter_rows(named=True)}

        assert by_siret["00000000000000"]["status"] == "success"
        assert by_siret["11111111111111"]["status"] == "success"
        assert by_siret["11111111111111"]["latitude"] == 48.869
        assert by_siret["22222222222222"]["status"] == "failed"
        assert by_siret["22222222222222"]["latitude"] is None
        assert by_siret["99999999999999"]["status"] == "not_in_sirene"

    def test_geocode_missing_sirets_handles_api_failure_gracefully(self, monkeypatch):
        today = date(2026, 5, 15)
        lf_decp = pl.LazyFrame(
            {
                "acheteur_id": ["11111111111111"],
                "titulaire_id": [None],
            }
        )
        lf_siret_latlong = pl.LazyFrame(
            {
                "siret": [],
                "latitude": [],
                "longitude": [],
                "source": [],
                "score": [],
                "geocoded_at": [],
                "status": [],
            },
            schema_overrides={
                "siret": pl.String,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "source": pl.String,
                "score": pl.Float64,
                "geocoded_at": pl.Date,
                "status": pl.String,
            },
        )
        lf_etab = pl.LazyFrame(
            {
                "siret": ["11111111111111"],
                "numeroVoieEtablissement": ["10"],
                "indiceRepetitionEtablissement": [None],
                "typeVoieEtablissement": ["RUE"],
                "libelleVoieEtablissement": ["FOO"],
                "codePostalEtablissement": ["75001"],
                "commune_code": ["75101"],
            }
        )

        _original_Client = httpx.Client

        def handler(request):
            return httpx.Response(500)

        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            "src.tasks.geocode.httpx.Client",
            lambda **kw: _original_Client(
                transport=transport,
                **{k: v for k, v in kw.items() if k != "transport"},
            ),
        )
        monkeypatch.setattr("src.tasks.enrich.date", _FrozenDate(today))

        updated = geocode_missing_sirets(lf_decp, lf_siret_latlong, lf_etab).collect()
        assert "11111111111111" not in set(updated["siret"].to_list())

    def test_add_type_marche(self):
        lf = pl.LazyFrame(
            {
                "uid": ["1", "2", "3", "4"],
                "codeCPV": ["1581791-1", "4587554-2", "4876655-5", "618765-3"],
            }
        )

        df_cible = pl.DataFrame(
            {
                "uid": ["1", "2", "3", "4"],
                "codeCPV": ["1581791-1", "4587554-2", "4876655-5", "618765-3"],
                "type": ["Fournitures", "Travaux", "Fournitures", "Services"],
            }
        )

        assert_frame_equal(
            add_type_marche(lf).collect(), df_cible, check_column_order=False
        )


class _FrozenDate:
    """Fige date.today() dans les tests."""

    def __init__(self, value):
        self._value = value

    def today(self):
        return self._value

    def __call__(self, *args, **kwargs):
        return date(*args, **kwargs)

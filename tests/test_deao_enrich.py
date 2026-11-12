import json

import polars as pl
import pytest

from src.config import BASE_DIR
from src.tasks.deao.enrich import (
    appliquer_dictionnaire_siret,
    enrich_acheteurs,
    enrichir_sirene,
    normaliser_nom_expr,
)
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA

SIRENE = BASE_DIR / "tests/data/sirene"


def _lf_ul() -> pl.LazyFrame:
    return pl.DataFrame(json.load(open(SIRENE / "unites_legales.json"))).lazy()


def _lf_etab() -> pl.LazyFrame:
    df = pl.DataFrame(json.load(open(SIRENE / "etablissements.json")))
    extra = df.head(1).with_columns(
        siret=pl.lit("20007695800011"),
        etablissement_nom=pl.lit(None, dtype=pl.String),
        commune_code=pl.lit("20004"),
        commune_nom=pl.lit("Ajaccio"),
        departement_code=pl.lit("2A"),
        departement_nom=pl.lit("Corse-du-Sud"),
        region_code=pl.lit("94"),
        region_nom=pl.lit("Corse"),
    )
    return pl.concat([df, extra]).lazy()


COG = pl.DataFrame(
    {
        "commune_code": ["29019", "20004", "73065"],
        "commune_nom": ["Brest", "Ajaccio", "Chambéry"],
        "departement_code": ["29", "2A", "73"],
        "departement_nom": ["Finistère", "Corse-du-Sud", "Savoie"],
        "region_code": ["53", "94", "84"],
        "region_nom": ["Bretagne", "Corse", "Auvergne-Rhône-Alpes"],
    }
)

SORTIE_GEO = [
    "acheteur_commune_code",
    "acheteur_commune_nom",
    "acheteur_departement_code",
    "acheteur_departement_nom",
    "acheteur_region_code",
    "acheteur_region_nom",
    "acheteur_latitude",
    "acheteur_longitude",
]


def _df(**cols) -> pl.DataFrame:
    n = len(next(iter(cols.values())))
    base = {
        "acheteur_id": [None] * n,
        "acheteur_nom": [None] * n,
        "acheteur_adresse_voie": [None] * n,
        "acheteur_adresse_cp": [None] * n,
        "acheteur_adresse_ville": [None] * n,
        "acheteur_adresse_nuts": [None] * n,
    }
    base.update(cols)
    return pl.DataFrame(base, schema={k: pl.String for k in base})


def test_normaliser_nom():
    df = pl.DataFrame({"n": ["  C.M.A.R.  Normandie ", "Hôtel-Dieu"]})
    assert df.select(normaliser_nom_expr("n"))["n"].to_list() == [
        "c m a r normandie",
        "hotel dieu",
    ]


def test_dictionnaire_cle_non_ambigue():
    df = pl.DataFrame(
        {
            "acheteur_id": ["13002799800017", None, None],
            "acheteur_nom": ["CMAR Normandie", "CMAR, NORMANDIE.", "Autre"],
            "acheteur_adresse_cp": ["14000", "14000", "14000"],
        }
    )
    out = appliquer_dictionnaire_siret(df)
    assert out["acheteur_id"].to_list() == ["13002799800017", "13002799800017", None]
    assert out["acheteur_id_source"].to_list() == ["avis", "dictionnaire", None]


def test_dictionnaire_cle_ambigue_ignoree():
    df = pl.DataFrame(
        {
            "acheteur_id": ["11111111100001", "22222222200002", None],
            "acheteur_nom": ["Mairie", "Mairie", "Mairie"],
            "acheteur_adresse_cp": ["14000", "14000", "14000"],
        }
    )
    out = appliquer_dictionnaire_siret(df)
    assert out["acheteur_id"].to_list() == ["11111111100001", "22222222200002", None]
    assert out["acheteur_id_source"].to_list() == ["avis", "avis", None]


def test_dictionnaire_nom_ou_cp_nul_ignore():
    df = pl.DataFrame(
        {
            "acheteur_id": ["11111111100001", None, None],
            "acheteur_nom": ["Mairie", "Mairie", None],
            "acheteur_adresse_cp": [None, None, "14000"],
        }
    )
    out = appliquer_dictionnaire_siret(df)
    assert out["acheteur_id"].to_list() == ["11111111100001", None, None]


def test_enrichir_sirene_nom_et_categorie():
    df = _df(
        acheteur_id=["20007695800011", "99999999900001"],
        acheteur_nom=["Nom avis", "Inconnu"],
    )
    out = enrichir_sirene(df, _lf_ul(), _lf_etab())
    assert out["acheteur_nom"].to_list() == ["Collectivité de Corse", "Inconnu"]
    assert out["acheteur_categorie"].to_list() == ["Région", None]
    assert out["_sirene_trouve"].to_list() == [True, False]
    assert out["acheteur_commune_code"].to_list() == ["20004", None]
    assert "acheteur_siren" not in out.columns
    assert out.height == 2


def _geocoder_factice(recus: list):
    def geocoder(adresses: pl.DataFrame) -> pl.DataFrame:
        recus.append(adresses)
        cles = adresses["adresse_cle"].to_list()
        rows = []
        for c in cles:
            if c.startswith("1 rue de siam"):
                rows.append((c, "29019", 48.39, -4.48, 0.9, "success"))
            else:
                rows.append((c, None, None, None, None, "failed"))
        return (
            pl.DataFrame(
                rows,
                schema=[
                    "adresse_cle",
                    "commune_code",
                    "latitude",
                    "longitude",
                    "score",
                    "status",
                ],
                orient="row",
            )
            .with_columns(geocoded_at=pl.lit(None, dtype=pl.Date))
            .cast(ADRESSE_GEOCODE_SCHEMA)
        )

    return geocoder


def test_geographie_par_geocodage_puis_replis():
    df = _df(
        acheteur_nom=["A", "B", "C"],
        acheteur_adresse_voie=["1 rue de Siam", "Route", None],
        acheteur_adresse_cp=["29200", "20137", None],
        acheteur_adresse_ville=["Brest", "Porto-Vecchio", None],
        acheteur_adresse_nuts=[None, None, "FRK27"],
    )
    recus = []
    out = enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, _geocoder_factice(recus))
    assert out.height == 3
    assert out["acheteur_commune_nom"].to_list() == ["Brest", None, None]
    assert out["acheteur_departement_code"].to_list() == ["29", "2A", "73"]
    assert out["acheteur_departement_nom"].to_list() == [
        "Finistère",
        "Corse-du-Sud",
        "Savoie",
    ]
    assert out["acheteur_region_code"].to_list() == ["53", "94", "84"]
    assert out["acheteur_latitude"].to_list() == [48.39, None, None]
    assert out["acheteur_latitude"].dtype == pl.Float64
    assert out["acheteur_id_source"].to_list() == [None, None, None]
    # la ligne C n'a pas d'adresse : elle n'est pas envoyée au géocodeur
    assert recus[0].columns == ["adresse_cle", "q", "postcode"]
    assert recus[0].height == 2
    assert recus[0]["q"].to_list()[0] == "1 rue de Siam Brest"
    assert not any(c.startswith("_") or c == "adresse_cle" for c in out.columns)


def test_ligne_trouvee_dans_sirene_n_est_pas_geocodee():
    df = _df(
        acheteur_id=["20007695800011", None],
        acheteur_nom=["X", "Y"],
        acheteur_adresse_voie=["1 rue de Siam", "2 rue Autre"],
        acheteur_adresse_cp=["29200", "29200"],
        acheteur_adresse_ville=["Brest", "Brest"],
    )
    recus = []
    out = enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, _geocoder_factice(recus))
    assert out.height == 2
    assert recus[0].height == 1
    assert "2 rue autre" in recus[0]["adresse_cle"][0]
    # valeurs SIRENE non écrasées
    assert out["acheteur_commune_nom"][0] == "Ajaccio"
    assert out["acheteur_id_source"].to_list() == ["avis", None]


def test_aucun_geocodage_si_tout_est_dans_sirene():
    df = _df(acheteur_id=["20007695800011"], acheteur_nom=["X"])

    def geocoder(_):
        raise AssertionError("ne doit pas être appelé")

    out = enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, geocoder)
    assert out["acheteur_categorie"].to_list() == ["Région"]


def test_enrichir_sirene_siret_duplique_dans_sirene():
    lf_etab = pl.concat([_lf_etab(), _lf_etab().filter(siret="20007695800011")])
    assert lf_etab.filter(siret="20007695800011").collect().height == 2
    df = _df(
        acheteur_id=["20007695800011", "99999999900001"],
        acheteur_nom=["Nom avis", "Inconnu"],
    )
    out = enrichir_sirene(df, _lf_ul(), lf_etab)
    assert out.height == 2
    assert out["acheteur_nom"].to_list() == ["Collectivité de Corse", "Inconnu"]


def test_enrich_acheteurs_nombre_de_lignes_modifie(monkeypatch):
    from src.tasks.deao import enrich

    monkeypatch.setattr(
        enrich, "enrichir_geographie", lambda df, cog, geocoder: pl.concat([df, df])
    )
    df = _df(acheteur_id=["20007695800011"], acheteur_nom=["Nom avis"])
    with pytest.raises(RuntimeError, match="nombre de lignes"):
        enrich.enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, geocoder=None)


def test_acheteur_etranger_sans_geo():
    df = _df(
        acheteur_id=["20007695800011", None, None],
        acheteur_nom=["Mairie de Test", "Mairie de Test", "Mairie de Test"],
        acheteur_adresse_voie=["1 rue de Siam", "ul. Testowa 1", "2 rue Ancienne"],
        acheteur_adresse_cp=["31000", "31-000", "29200"],
        acheteur_adresse_ville=["Toulouse", "Varsovie", "Brest"],
        acheteur_adresse_nuts=[None, "PL911", None],
    ).with_columns(acheteur_pays=pl.Series(["FRA", "POL", None], dtype=pl.String))
    postcodes: list = []

    def geocoder_espion(adresses: pl.DataFrame) -> pl.DataFrame:
        postcodes.extend(adresses["postcode"].to_list())
        return _geocoder_factice([])(adresses)

    out = enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, geocoder=geocoder_espion)
    pol = out.filter(pl.col("acheteur_pays") == "POL").row(0, named=True)
    assert pol["acheteur_id"] is None and pol["acheteur_id_source"] is None
    assert pol["acheteur_departement_code"] is None
    assert pol["acheteur_adresse_cp"] is None
    assert all("31-000" not in q for q in postcodes)
    assert postcodes == ["29200"]
    # pays nul = français : traitement inchangé
    assert out["acheteur_id"][0] == "20007695800011"
    assert out["acheteur_departement_code"][2] == "29"


def test_neutralisation_independante_du_perimetre_de_requete(monkeypatch):
    """PAYS_FRANCAIS fixe la neutralisation ; JOUE_COUNTRY_CODES ne fait que la requête TED."""
    from src.tasks.deao import enrich

    monkeypatch.setattr(enrich, "JOUE_COUNTRY_CODES", ("FRA",), raising=False)
    df = pl.DataFrame(
        {
            "acheteur_pays": ["PYF", "DEU"],
            "acheteur_id": ["a", "b"],
            "acheteur_adresse_voie": ["v", "v"],
            "acheteur_adresse_cp": ["98700", "75001"],
            "acheteur_adresse_ville": ["x", "y"],
            "acheteur_adresse_nuts": ["n", "n"],
        }
    )
    out = enrich.neutraliser_acheteurs_etrangers(df)
    assert out["acheteur_id"].to_list() == ["a", None]

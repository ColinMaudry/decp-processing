from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from src.config import SourceDeao
from src.tasks.deao.dedoublonnage import dedoublonner_sources, id_consultation_expr
from src.tasks.deao.schema import published_schema

MEGALIS = "https://marches.megalis.bretagne.bzh"
MAXIMILIEN = "https://marches.maximilien.fr"
REGISTRE = (
    SourceDeao("boamp", "BOAMP", True, "r-boamp", 1),
    SourceDeao("joue", "JOUE", True, "r-joue", 1),
    SourceDeao("megalis_bretagne", "Mégalis", False, "r-meg", 1, url_base=MEGALIS),
    SourceDeao("maximilien", "Maximilien", False, "r-max", 1, url_base=MAXIMILIEN),
)
PARIS = ZoneInfo("Europe/Paris")
URL_BOAMP = (
    f"{MEGALIS}/?page=Entreprise.EntrepriseAdvancedSearch&AllCons&id=242752"
    "&orgAcronyme=a4h"
)
URL_MEGALIS = f"{MEGALIS}/entreprise/consultation/242752?orgAcronyme=a4h"
SIRET = "21350238800019"


def _lignes(source: str, id_: str, lots=("0",), **valeurs) -> list[dict]:
    valeurs.setdefault("dateLimiteReponse", datetime(2026, 12, 1, 12, tzinfo=PARIS))
    return [
        {
            "uid": f"{source}_{id_}_{lot}",
            "sourceDataset": source,
            "id": id_,
            "lotId": lot,
            **valeurs,
        }
        for lot in lots
    ]


def _df(*groupes: list[dict]) -> pl.DataFrame:
    lignes = [ligne for groupe in groupes for ligne in groupe]
    schema = published_schema()
    return pl.DataFrame(
        [{c: ligne.get(c) for c in schema} for ligne in lignes], schema=schema
    )


def _plateforme_megalis(**valeurs):
    valeurs = {
        "urlConsultation": URL_MEGALIS,
        "urlDce": "https://s3/x.zip",
        **valeurs,
    }
    return _lignes("megalis_bretagne", "242752", **valeurs)


def test_id_consultation_expr():
    df = pl.DataFrame(
        {
            "urlConsultation": [
                URL_BOAMP,
                URL_MEGALIS,
                "https://MARCHES.megalis.bretagne.bzh/x?id=12",
                f"{MAXIMILIEN}/entreprise/consultation/242752",
                f"{MEGALIS}/",
                None,
            ]
        }
    )
    assert df.select(id_consultation_expr(MEGALIS))["urlConsultation"].to_list() == [
        "242752",
        "242752",
        "12",
        None,
        None,
        None,
    ]


def test_fusion_exacte_par_url():
    df = _df(
        _lignes("boamp", "26-1", lots=("1", "2"), urlConsultation=URL_BOAMP),
        _plateforme_megalis(),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert set(df["sourceDataset"]) == {"boamp"} and df.height == 2
    assert df["uid"].to_list() == ["boamp_26-1_1", "boamp_26-1_2"]
    assert df["id"].to_list() == ["26-1"] * 2
    assert df["urlDce"].to_list() == ["https://s3/x.zip"] * 2
    assert df["urlConsultation"][0] == URL_MEGALIS
    assert stats["fusion_exacte:megalis_bretagne"] == 1
    assert stats["dedoublonnage_retire"] == 1
    assert stats["seule:megalis_bretagne"] == 0


def test_fusion_exacte_plusieurs_chaines_boamp():
    df = _df(
        _lignes("boamp", "26-1", urlConsultation=URL_BOAMP),
        _lignes("boamp", "26-9", urlConsultation=URL_BOAMP),
        _plateforme_megalis(),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df["sourceDataset"].to_list() == ["boamp", "boamp"]
    assert df["urlDce"].to_list() == ["https://s3/x.zip"] * 2
    assert stats["fusion_exacte:megalis_bretagne"] == 1
    assert stats["dedoublonnage_retire"] == 1


def test_url_autre_hote_pas_de_fusion():
    df = _df(
        _lignes(
            "boamp",
            "26-1",
            urlConsultation=f"{MAXIMILIEN}/entreprise/consultation/242752",
        ),
        _plateforme_megalis(),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 2
    assert set(df["sourceDataset"]) == {"boamp", "megalis_bretagne"}
    assert stats["seule:megalis_bretagne"] == 1
    assert stats["dedoublonnage_retire"] == 0


def _strict(source, id_, ref="2026-21", siret=SIRET, **valeurs):
    return _lignes(source, id_, referenceAcheteur=ref, acheteur_id=siret, **valeurs)


def test_fusion_stricte():
    df = _df(
        _strict("boamp", "26-1", ref="2026 21", urlConsultation=f"{MEGALIS}/"),
        _strict(
            "megalis_bretagne",
            "242752",
            ref="2026-21",
            urlConsultation=URL_MEGALIS,
            urlDce="https://s3/x.zip",
            # même jour, autre heure
            dateLimiteReponse=datetime(2026, 12, 1, 16, tzinfo=PARIS),
        ),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df["sourceDataset"].to_list() == ["boamp"]
    assert df["urlConsultation"].to_list() == [URL_MEGALIS]
    assert df["urlDce"].to_list() == ["https://s3/x.zip"]
    assert stats["fusion_stricte:megalis_bretagne"] == 1
    assert stats["fusion_exacte:megalis_bretagne"] == 0
    assert stats["dedoublonnage_retire"] == 1


def test_stricte_ambigue_pas_de_fusion():
    df = _df(
        _strict("boamp", "26-1", urlConsultation=f"{MEGALIS}/"),
        _strict("boamp", "26-2", urlConsultation=f"{MEGALIS}/"),
        _strict("megalis_bretagne", "242752", urlConsultation=URL_MEGALIS),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 3
    assert stats["fusion_stricte:megalis_bretagne"] == 0
    assert stats["seule:megalis_bretagne"] == 1
    assert stats["dedoublonnage_retire"] == 0


def test_stricte_ambigue_cote_plateformes_pas_de_fusion():
    df = _df(
        _strict("boamp", "26-1"),
        _strict("megalis_bretagne", "242752"),
        _strict("maximilien", "777"),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 3
    assert stats["seule:megalis_bretagne"] == 1 and stats["seule:maximilien"] == 1


def test_stricte_siret_nul_pas_de_fusion():
    df = _df(
        _strict("boamp", "26-1", siret=None),
        _strict("megalis_bretagne", "242752", siret=None),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 2
    assert stats["fusion_stricte:megalis_bretagne"] == 0
    assert stats["seule:megalis_bretagne"] == 1


def test_stricte_jour_different_pas_de_fusion():
    df = _df(
        _strict("boamp", "26-1"),
        _strict(
            "megalis_bretagne",
            "242752",
            dateLimiteReponse=datetime(2026, 12, 2, 12, tzinfo=PARIS),
        ),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 2


def test_stricte_url_boamp_vers_autre_consultation_pas_de_fusion():
    """L'URL BOAMP désigne une autre consultation de la plateforme : pas de
    rapprochement strict, même si référence, SIRET et date concordent."""
    df = _df(
        _strict("boamp", "26-1", urlConsultation=f"{MEGALIS}/?page=x&id=111"),
        _strict("megalis_bretagne", "242752", urlConsultation=URL_MEGALIS),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 2


def test_valeur_boamp_non_ecrasee():
    df = _df(
        _lignes(
            "boamp",
            "26-1",
            urlConsultation=URL_BOAMP,
            referenceAcheteur="A",
            acheteur_codePostal="35000",
        ),
        _plateforme_megalis(
            referenceAcheteur="B",
            acheteur_codePostal="29000",
            urlReglementConsultation="https://rc",
        ),
    )
    df, _ = dedoublonner_sources(df, REGISTRE)
    assert df["referenceAcheteur"].to_list() == ["A"]
    assert df["acheteur_codePostal"].to_list() == ["35000"]
    assert df["urlReglementConsultation"].to_list() == ["https://rc"]


def test_sans_plateforme_identite():
    df = _df(
        _lignes("boamp", "26-1", urlConsultation=URL_BOAMP),
        _lignes("joue", "123-2026"),
    )
    resultat, stats = dedoublonner_sources(df, REGISTRE)
    assert resultat.equals(df)
    assert stats["dedoublonnage_retire"] == 0


def test_stricte_ambiguite_non_masquee_par_appariement_exact():
    """BOAMP A apparié exactement à 242752 ; BOAMP B (URL racine) partage la clé
    stricte avec 242752 et 999 : la clé est ambiguë, B n'est pas fusionné."""
    df = _df(
        _strict("boamp", "26-1", urlConsultation=URL_BOAMP),
        _strict("boamp", "26-2", urlConsultation=f"{MEGALIS}/"),
        _strict("megalis_bretagne", "242752", urlConsultation=URL_MEGALIS),
        _strict(
            "megalis_bretagne",
            "999",
            urlConsultation=f"{MEGALIS}/entreprise/consultation/999",
        ),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert stats["fusion_exacte:megalis_bretagne"] == 1
    assert stats["fusion_stricte:megalis_bretagne"] == 0
    assert stats["seule:megalis_bretagne"] == 1
    assert df.filter(pl.col("sourceDataset") == "megalis_bretagne")["id"].to_list() == [
        "999"
    ]
    assert df.height == 3


def test_stricte_url_vers_plateforme_absente_pas_de_fusion():
    """Maximilien écarté du run : une URL BOAMP désignant une consultation
    Maximilien n'est pas candidate au rapprochement strict avec Mégalis."""
    df = _df(
        _strict("boamp", "26-1", urlConsultation=f"{MAXIMILIEN}/?page=x&id=555"),
        _strict("megalis_bretagne", "242752", urlConsultation=URL_MEGALIS),
    )
    df, stats = dedoublonner_sources(df, REGISTRE)
    assert df.height == 2
    assert stats["fusion_stricte:megalis_bretagne"] == 0
    assert stats["seule:megalis_bretagne"] == 1

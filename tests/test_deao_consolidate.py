from collections import Counter
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.tasks.deao.consolidate import (
    RecouvrementIncoherent,
    consolidate,
    ecarter_recouvrement,
    resolve_roots,
    statut_expr,
)
from src.tasks.deao.schema import AVIS_LOT_SCHEMA, PUBLISHED_FIELDS, conform_avis
from src.tasks.deao.values import PARIS
from src.tasks.scrap.deao.boamp_joue.boamp.parse import parse_day
from src.tasks.scrap.deao.boamp_joue.joue.identifiants import resoudre_avis_precedents
from src.tasks.scrap.deao.boamp_joue.joue.parse import parse_day as parse_day_joue

FIXTURES = Path(__file__).parent / "data" / "deao"
RUN = date(2026, 10, 15)

DEFAUTS = dict(
    sourceDataset="boamp",
    idAvis="I",
    typeAvis="initial",
    avisComplet=True,
    preInformation=False,
    formatSource="eforms",
    lotId="1",
    datePublicationAvis=date(2026, 10, 1),
)


def dl(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 12, 0, tzinfo=PARIS)


def avis(**kw) -> dict:
    """Ligne avis-lot : par défaut un avis initial complet valide."""
    row = {name: None for name in AVIS_LOT_SCHEMA}
    row.update(DEFAUTS)
    row["urlAvis"] = (
        f'https://www.boamp.fr/pages/avis/?q=idweb:"{kw.get("idAvis", "I")}"'
    )
    row["sourceFile"] = f"https://example.org/{kw.get('idAvis', 'I')}.xml"
    row.update(kw)
    if row["typeAvis"] != "initial" and "preInformation" not in kw:
        row["preInformation"] = None
    return row


def frame(rows: list[dict]) -> pl.DataFrame:
    return conform_avis(pl.DataFrame(rows, schema=AVIS_LOT_SCHEMA))


def test_resolve_roots_chaine_racine_absente_et_cycle():
    df = frame(
        [
            avis(idAvis="B", avisPrecedent="A"),
            avis(idAvis="C", avisPrecedent="B"),
            avis(idAvis="E", avisPrecedent="Z"),  # Z absent
            avis(idAvis="X", avisPrecedent="Y"),
            avis(idAvis="Y", avisPrecedent="X"),
        ]
    )
    roots = resolve_roots(df)
    assert set(roots.columns) == {"sourceDataset", "idAvis", "id"}
    r = dict(roots.select("idAvis", "id").iter_rows())
    assert r["C"] == "A" and r["B"] == "A" and r["E"] == "Z"
    assert r["X"] == "X" and r["Y"] == "Y"


def test_consolidate_compte_les_cycles():
    df = frame(
        [avis(idAvis="X", avisPrecedent="Y"), avis(idAvis="Y", avisPrecedent="X")]
    )
    assert consolidate(df, RUN, 90).stats["cycle"] == 2


def test_rectificatif_legacy_s_applique_a_tous_les_lots():
    df = frame(
        [
            avis(idAvis="I", lotId="1", dateLimiteReponse=dl(2026, 10, 20)),
            avis(idAvis="I", lotId="2", dateLimiteReponse=dl(2026, 10, 20)),
            avis(
                idAvis="R",
                avisPrecedent="I",
                typeAvis="rectificatif",
                avisComplet=False,
                lotId="0",
                infosRectificatif="report",
                datePublicationAvis=date(2026, 10, 14),
            ),
        ]
    )
    out = consolidate(df, RUN, 90).df.sort("uid")
    assert out["uid"].to_list() == ["boamp_I_1", "boamp_I_2"]
    assert out["rectifie"].all() and out["idDernierAvis"].to_list() == ["R", "R"]
    assert out["infosRectificatifs"][0].to_list() == ["report"]
    assert out["avisLies"][0].to_list() == ["I", "R"]
    assert out["datePublicationInitiale"].to_list() == [date(2026, 10, 1)] * 2
    assert out["dateDerniereModification"].to_list() == [date(2026, 10, 14)] * 2
    assert out["statut"].to_list() == ["ouvert", "ouvert"]
    # urlAvis : celle du dernier avis ; sourceFile : celle de la ligne descriptive
    assert out["urlAvis"][0].endswith('"R"')
    assert out["sourceFile"][0].endswith("/I.xml")


def test_dernier_avis_complet_et_lot_absent_du_dernier():
    # I : lots 1 (objet "v1") et 2 ; R eForms complet : lot 1 seulement (objet "v2")
    df = frame(
        [
            avis(idAvis="I", lotId="1", objet="v1"),
            avis(idAvis="I", lotId="2", objet="v1"),
            avis(
                idAvis="R",
                avisPrecedent="I",
                typeAvis="rectificatif",
                lotId="1",
                objet="v2",
                datePublicationAvis=date(2026, 10, 10),
            ),
        ]
    )
    out = consolidate(df, RUN, 90).df.sort("uid")
    assert out["uid"].to_list() == ["boamp_I_1", "boamp_I_2"]
    assert out["objet"].to_list() == ["v2", "v1"]
    assert out["sourceFile"][1].endswith("/I.xml")
    assert out["idDernierAvis"].to_list() == ["R", "R"]
    assert out["infosRectificatifs"][0].to_list() == []


def test_colonnes_de_sortie():
    out = consolidate(frame([avis()]), RUN, 90).df
    enrichissement = {
        "acheteur_id_source",
        "acheteur_codePostal",
        "acheteur_categorie",
        "acheteur_latitude",
        "acheteur_longitude",
    }
    attendues = [
        f.name
        for f in PUBLISHED_FIELDS
        if f.name not in enrichissement
        and not f.closed_only
        and not f.name.startswith(
            ("acheteur_commune_", "acheteur_departement_", "acheteur_region_")
        )
    ] + [
        "formatSource",
        "acheteur_adresse_voie",
        "acheteur_adresse_cp",
        "acheteur_adresse_ville",
        "acheteur_adresse_nuts",
        "acheteur_pays",
    ]
    assert out.columns == attendues
    published = {f.name: f.dtype for f in PUBLISHED_FIELDS}
    for name in out.columns:
        if name in published:
            assert out.schema[name] == published[name], name


@pytest.mark.parametrize(
    "cas, attendu",
    [
        (dict(annule=True), "annule"),
        (dict(preInformation=True), "pre-information"),
        (dict(preInformation=True, annule=True), "annule"),
        (dict(dateLimiteReponse=dl(2026, 10, 14)), "clos"),
        (dict(dateLimiteReponse=dl(2026, 10, 15)), "ouvert"),
        # 00:30 à Paris le 15 = 22:30 UTC le 14 : c'est la date à Paris qui compte
        (dict(dateLimiteReponse=datetime(2026, 10, 15, 0, 30, tzinfo=PARIS)), "ouvert"),
        (
            dict(dateLimiteReponse=None, datePublicationInitiale=date(2026, 7, 18)),
            "ouvert",
        ),  # 89 jours
        (
            dict(dateLimiteReponse=None, datePublicationInitiale=date(2026, 7, 17)),
            "clos",
        ),  # 90 jours
    ],
)
def test_statut(cas, attendu):
    row = dict(
        annule=False,
        preInformation=False,
        dateLimiteReponse=dl(2026, 10, 20),
        datePublicationInitiale=date(2026, 10, 1),
    )
    row.update(cas)
    df = pl.DataFrame(
        [row],
        schema={
            "annule": pl.Boolean,
            "preInformation": pl.Boolean,
            "dateLimiteReponse": pl.Datetime("us", "Europe/Paris"),
            "datePublicationInitiale": pl.Date,
        },
    )
    assert df.select(statut_expr(RUN, 90))["statut"].item() == attendu


def test_chaine_annulation_seule_ecartee():
    df = frame(
        [
            avis(idAvis="I"),
            avis(
                idAvis="A",
                avisPrecedent="ANCIEN",
                typeAvis="annulation",
                avisComplet=False,
                lotId="0",
            ),
        ]
    )
    res = consolidate(df, RUN, 90)
    assert res.df["uid"].to_list() == ["boamp_I_1"]
    assert res.stats["chaine_annulation_seule"] == 1


def test_chaine_sans_avis_complet_prend_le_dernier_rectificatif():
    df = frame(
        [
            avis(
                idAvis="R1",
                avisPrecedent="ANCIEN",
                typeAvis="rectificatif",
                avisComplet=False,
                lotId="0",
                objet="r1",
                datePublicationAvis=date(2026, 10, 2),
            ),
            avis(
                idAvis="R2",
                avisPrecedent="R1",
                typeAvis="rectificatif",
                avisComplet=False,
                lotId="0",
                objet="r2",
                datePublicationAvis=date(2026, 10, 5),
            ),
        ]
    )
    out = consolidate(df, RUN, 90).df
    assert out["uid"].to_list() == ["boamp_ANCIEN_0"]
    assert out["objet"].to_list() == ["r2"]
    # avis racine absent : date du plus ancien avis disponible ; pas de pré-information
    assert out["datePublicationInitiale"].to_list() == [date(2026, 10, 2)]
    assert out["statut"].to_list() == ["ouvert"]


def test_pre_information_heritee_et_annulation():
    df = frame(
        [
            avis(idAvis="P", preInformation=True),
            avis(
                idAvis="R",
                avisPrecedent="P",
                typeAvis="rectificatif",
                datePublicationAvis=date(2026, 10, 3),
            ),
        ]
    )
    assert consolidate(df, RUN, 90).df["statut"].to_list() == ["pre-information"]
    df = pl.concat(
        [
            df,
            frame(
                [
                    avis(
                        idAvis="A",
                        avisPrecedent="R",
                        typeAvis="annulation",
                        avisComplet=False,
                        lotId="0",
                        datePublicationAvis=date(2026, 10, 4),
                    )
                ]
            ),
        ]
    )
    out = consolidate(df, RUN, 90).df
    assert out["statut"].to_list() == ["annule"]
    assert out["idDernierAvis"].to_list() == ["A"]


def test_date_publication_nulle_perd_contre_une_date():
    df = frame(
        [
            avis(idAvis="I"),
            avis(
                idAvis="R",
                avisPrecedent="I",
                typeAvis="rectificatif",
                objet="sans date",
                datePublicationAvis=None,
            ),
        ]
    )
    out = consolidate(df, RUN, 90).df
    assert out["idDernierAvis"].to_list() == ["I"]
    assert out["objet"].to_list() == [None]


def test_uid_doublon():
    df = frame([avis(idAvis="I", objet="a"), avis(idAvis="I", objet="b")])
    res = consolidate(df, RUN, 90)
    assert res.df.height == 1
    assert res.stats["uid_doublon"] == 1


def test_uid_doublon_choix_deterministe():
    a = avis(idAvis="I", lotIntitule="Lot A", objet="a")
    b = avis(idAvis="I", lotIntitule="Lot B", objet="b")
    for lignes in ([a, b], [b, a]):
        res = consolidate(frame(lignes), RUN, 90)
        # même ligne conservée quel que soit l'ordre d'entrée : le plus petit intitulé
        assert res.df["lotIntitule"].to_list() == ["Lot A"]


def test_consolidate_fixtures():
    df, _ = parse_day({p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")})
    out = consolidate(df, RUN, 90).df
    assert out.height == 31 and out["uid"].is_unique().all()
    r = out.filter(pl.col("uid") == "boamp_26-87868_0").row(0, named=True)
    assert (r["idDernierAvis"], r["rectifie"], r["statut"]) == (
        "26-99074",
        True,
        "ouvert",
    )
    assert r["dateLimiteReponse"] == datetime(2026, 10, 30, 16, 30, tzinfo=PARIS)
    assert (r["datePublicationInitiale"], r["dateDerniereModification"]) == (
        date(2026, 9, 12),
        date(2026, 10, 14),
    )
    assert out.filter(pl.col("id") == "26-99177")["statut"].unique().to_list() == [
        "annule"
    ]
    assert out.filter(pl.col("id") == "26-92574").height == 8
    assert set(out.filter(pl.col("id").is_in(["26-97189", "26-94806"]))["statut"]) == {
        "pre-information"
    }
    assert set(out.filter(pl.col("id") == "26-98723")["statut"]) == {"ouvert"}


def _boamp(*numeros: str) -> pl.DataFrame:
    df, _ = parse_day(
        {f"{n}.xml": (FIXTURES / f"{n}.xml").read_bytes() for n in numeros}
    )
    return df


def _joue(*numeros: str) -> pl.DataFrame:
    df, _ = parse_day_joue(
        {f"{n}.xml": (FIXTURES / "joue" / f"{n}.xml").read_bytes() for n in numeros}
    )
    return resoudre_avis_precedents(df, Counter(), resolver=lambda ids: {})


def _union(*dfs: pl.DataFrame) -> pl.DataFrame:
    return pl.concat(dfs, how="vertical_relaxed")


def test_chaine_joue_recoupant_boamp_ecartee():
    df = _union(_boamp("26-87868", "26-99074"), _joue("631605-2026", "709755-2026"))
    c = consolidate(df, RUN, 90)
    assert not c.df.filter(pl.col("sourceDataset") == "joue").height
    assert c.df.filter(pl.col("sourceDataset") == "boamp").height
    assert c.stats["joue:chaine_recouvrement"] == 1


def test_chaine_joue_seule_conservee():
    df = _union(_boamp("26-87868"), _joue("631605-2026", "677195-2026", "706679-2026"))
    c = consolidate(df, RUN, 90)
    joue = c.df.filter(pl.col("sourceDataset") == "joue")["uid"].to_list()
    assert joue and all(u.startswith("joue_677195-2026_") for u in joue)
    assert c.stats["joue:chaine_recouvrement"] == 1
    assert c.stats["joue:chaine_conservee"] == 1


def test_garde_fou_partitions_boamp_anciennes():
    boamp = _boamp("26-87868", "26-99074").with_columns(
        pl.lit(None, dtype=pl.String).alias("idEforms")
    )
    with pytest.raises(RecouvrementIncoherent, match="idEforms manquant"):
        consolidate(_union(boamp, _joue("631605-2026")), RUN, 90)


def test_garde_fou_aucun_recouvrement():
    with pytest.raises(RecouvrementIncoherent, match="Aucune chaîne JOUE"):
        consolidate(
            _union(_boamp("26-87868"), _joue("677195-2026", "706679-2026")), RUN, 90
        )


def _lignes_recouvrement(n_recents: int, n_anciens_sans_id: int) -> pl.DataFrame:
    """BOAMP eForms récents (avec idEforms) + anciens (idEforms nul) + JOUE recoupant le 1er récent."""
    rows = [avis(idAvis=f"R{i}", idEforms=f"e-{i}") for i in range(n_recents)]
    rows += [avis(idAvis=f"A{i}", idEforms=None) for i in range(n_anciens_sans_id)]
    rows.append(
        avis(
            sourceDataset="joue",
            idAvis="J1",
            idEforms="e-0",
            formatSource="eforms",
        )
    )
    return frame(rows).with_columns(id=pl.col("idAvis"))


def test_garde_fou_partitions_boamp_mixtes():
    """Récents avec idEforms (re-crawl) + anciens majoritaires sans : doublons probables."""
    df = _lignes_recouvrement(n_recents=5, n_anciens_sans_id=50)
    with pytest.raises(RecouvrementIncoherent, match="idEforms manquant"):
        ecarter_recouvrement(df, Counter())


def test_garde_fou_tolere_une_ligne_eforms_sans_idEforms():
    df = _lignes_recouvrement(n_recents=150, n_anciens_sans_id=1)
    out = ecarter_recouvrement(df, Counter())
    assert not out.filter(pl.col("sourceDataset") == "joue").height


def test_sans_joue_pas_de_garde_fou():
    consolidate(_boamp("26-87868"), RUN, 90)
    consolidate(
        _boamp("26-87868").with_columns(
            pl.lit(None, dtype=pl.String).alias("idEforms")
        ),
        RUN,
        90,
    )

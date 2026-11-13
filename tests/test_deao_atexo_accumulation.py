from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.tasks.deao.schema import published_schema
from src.tasks.scrap.deao.atexo.accumulation import (
    dce_a_telecharger,
    fusionner,
    lots_a_relire,
    recherche_complete,
)
from src.tasks.scrap.deao.atexo.lignes import lignes_consultation
from src.tasks.scrap.deao.atexo.parse import ConsultationListe, Lot

TODAY = date(2026, 11, 13)
PARIS = ZoneInfo("Europe/Paris")
FUTUR = datetime(2026, 12, 1, 12, 0, tzinfo=PARIS)
PASSE = datetime(2026, 11, 1, 12, 0, tzinfo=PARIS)


def _cons(ref="1", limite=FUTUR, nombre_lots=0) -> ConsultationListe:
    return ConsultationListe(
        ref_cons=ref, org_cons="o", procedure_code="MAPA", categorie="Services",
        date_mise_en_ligne=date(2026, 10, 1), reference="r", intitule="i", objet="o",
        organisme="Org", code_postal="35000", ville="Rennes", nombre_lots=nombre_lots,
        lieux_execution=["35"], date_limite=limite,
    )  # fmt: skip


def _df(*consultations, statut=None, lots=None) -> pl.DataFrame:
    lignes = []
    for c in consultations:
        for ligne in lignes_consultation("p", "https://p", c, None, lots or [], None):
            lignes.append({**ligne, "statut": statut})
    return (
        pl.DataFrame(lignes, schema={**published_schema()}, strict=False)
        if lignes
        else pl.DataFrame(schema=published_schema())
    )


def _statuts(df):
    return dict(zip(df["id"], df["statut"]))


def test_nouvelle_ajoutee_ancienne_conservee():
    precedent = _df(_cons("1"), statut="ouvert")
    nouvelles = _df(_cons("2"))
    res = fusionner(precedent, nouvelles, {"1", "2"}, TODAY, True)
    assert _statuts(res) == {"1": "ouvert", "2": "ouvert"}
    assert res["uid"].to_list() == sorted(res["uid"].to_list())
    assert list(res.schema.items()) == list(published_schema().items())


def test_nouvelles_remplacent_les_lignes_precedentes():
    lots = [Lot("1", "a", None, None, None), Lot("2", "b", None, None, None)]
    precedent = _df(_cons("1", nombre_lots=2), statut="ouvert", lots=lots)
    nouvelles = _df(_cons("1", nombre_lots=1), lots=lots[:1])
    res = fusionner(precedent, nouvelles, {"1"}, TODAY, True)
    assert res["lotId"].to_list() == ["1"]


def test_disparue_avant_date_limite_annulee():
    precedent = _df(_cons("1"), statut="ouvert")
    res = fusionner(precedent, _df(), set(), TODAY, True)
    assert _statuts(res) == {"1": "annule"}
    # et le reste
    res = fusionner(res, _df(), set(), TODAY, True)
    assert _statuts(res) == {"1": "annule"}


def test_disparue_apres_date_limite_close():
    precedent = _df(_cons("1", limite=PASSE), statut="ouvert")
    res = fusionner(precedent, _df(), set(), TODAY, True)
    assert _statuts(res) == {"1": "clos"}


def test_vue_apres_date_limite_close():
    precedent = _df(_cons("1"), statut="ouvert")
    res = fusionner(precedent, _df(_cons("1", limite=PASSE)), {"1"}, TODAY, True)
    assert _statuts(res) == {"1": "clos"}


def test_recherche_incomplete_aucune_annulation():
    precedent = _df(_cons("1"), statut="ouvert")
    res = fusionner(precedent, _df(), set(), TODAY, False)
    assert _statuts(res) == {"1": "ouvert"}


def test_annulee_revue_redevient_ouverte():
    precedent = _df(_cons("1"), statut="annule")
    res = fusionner(precedent, _df(_cons("1")), {"1"}, TODAY, True)
    assert _statuts(res) == {"1": "ouvert"}


def test_sans_date_limite_recent_ouvert_ancien_clos():
    recent = _cons("recent", limite=None)  # publiée le 2026-10-01 : 43 jours
    ancien = _cons("ancien", limite=None)
    ancien = ConsultationListe(
        **{**ancien.__dict__, "date_mise_en_ligne": date(2026, 6, 1)}
    )
    precedent = _df(recent, ancien, statut="ouvert")
    vues = fusionner(precedent, precedent, {"recent", "ancien"}, TODAY, True, 90)
    assert _statuts(vues) == {"recent": "ouvert", "ancien": "clos"}
    # même règle que la plateforme les voie encore ou non
    disparues = fusionner(precedent, _df(), set(), TODAY, True, 90)
    assert _statuts(disparues) == {"recent": "ouvert", "ancien": "clos"}


def test_sans_date_limite_jamais_annule():
    precedent = _df(_cons("1", limite=None), statut="ouvert")
    res = fusionner(precedent, _df(), set(), TODAY, True, 10)
    assert _statuts(res) == {"1": "clos"}


def test_recherche_complete_seuil():
    precedent = _df(*[_cons(str(i)) for i in range(620)], statut="ouvert")
    assert recherche_complete(200, 200, precedent, TODAY) is False
    assert recherche_complete(310, 310, precedent, TODAY) is True
    assert recherche_complete(310, 400, precedent, TODAY) is False


def test_recherche_complete_compte_les_ouvertes_seulement():
    precedent = pl.concat(
        [
            _df(*[_cons(str(i)) for i in range(100)], statut="clos"),
            _df(_cons("a"), statut="ouvert"),
        ]
    )
    assert recherche_complete(1, 1, precedent, TODAY) is True


def test_lots_a_relire():
    lots = [Lot("1", "a", None, None, None), Lot("2", "b", None, None, None)]
    precedent = pl.concat(
        [
            _df(_cons("stable", nombre_lots=2), statut="ouvert", lots=lots),
            _df(_cons("date", nombre_lots=2), statut="ouvert", lots=lots),
            _df(_cons("nb", nombre_lots=2), statut="ouvert", lots=lots),
        ]
    )
    liste = [
        _cons("stable", nombre_lots=2),
        _cons("date", limite=PASSE, nombre_lots=2),
        _cons("nb", nombre_lots=3),
        _cons("nouvelle", nombre_lots=1),
    ]
    assert lots_a_relire(liste, precedent) == {"date", "nb", "nouvelle"}


@pytest.mark.parametrize(
    "affichee,archivee,url,attendu",
    [
        ("72,09 Mo", None, None, True),
        ("72,09 Mo", "72,09 Mo", "https://s3/x.zip", False),
        ("73,10 Mo", "72,09 Mo", "https://s3/x.zip", True),
        ("72,09 Mo", "72,09 Mo", None, True),
        (None, None, None, False),
        ("72,09 Mo", "72,09 Mo", "https://s3/x.zip", False),
    ],
)
def test_dce_a_telecharger(affichee, archivee, url, attendu):
    assert dce_a_telecharger(affichee, archivee, url) is attendu

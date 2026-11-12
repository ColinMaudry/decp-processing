from collections import Counter

import polars as pl

from src.tasks.deao.report import lignes_rapport, publier_rapport, taux_remplissage


def _df():
    return pl.DataFrame(
        {
            "formatSource": ["eforms", "eforms", "legacy"],
            "acheteur_id": ["1", None, None],
            "acheteur_id_source": ["avis", None, "dictionnaire"],
            "statut": ["ouvert", "clos", "clos"],
            "techniques": [["Accord-cadre"], [], []],
        }
    )


def test_taux_remplissage():
    t = taux_remplissage(_df().select("formatSource", "acheteur_id", "techniques"))
    assert t.columns == ["colonne", "formatSource", "taux"]
    assert (
        t.filter(
            (pl.col("colonne") == "acheteur_id") & (pl.col("formatSource") == "eforms")
        )["taux"][0]
        == 0.5
    )
    assert (
        t.filter(
            (pl.col("colonne") == "techniques") & (pl.col("formatSource") == "legacy")
        )["taux"][0]
        == 0.0
    )


def test_taux_remplissage_sans_colonne_par():
    t = taux_remplissage(pl.DataFrame({"acheteur_id": ["1", None]}))
    assert t["formatSource"].to_list() == ["tous"]
    assert t["taux"].to_list() == [0.5]


def test_lignes_rapport():
    stats = Counter({"lu": 5, "retenu": 3, "exclu:ATTRIBUTION": 2, "autre": 9})
    lignes = {r["indicateur"]: r["valeur"] for r in lignes_rapport(stats, _df())}
    assert lignes["lu"] == 5
    assert lignes["exclu:ATTRIBUTION"] == 2
    assert "autre" not in lignes
    assert lignes["statut:clos"] == 2
    assert lignes["acheteur_id_source:null"] == 1
    assert lignes["remplissage:acheteur_id:eforms"] == 0.5


def test_publier_rapport_hors_contexte_prefect():
    publier_rapport([{"indicateur": "lu", "valeur": 1}])


def test_lignes_rapport_compteurs_du_run():
    stats = Counter(
        cycle=1,
        jour_traite=30,
        erreur_mapping=2,
        sans_date_publication=3,
        jour_vide=4,
        inconnu=9,
    )
    lignes = {
        ligne["indicateur"]: ligne["valeur"]
        for ligne in lignes_rapport(stats, pl.DataFrame())
    }
    assert lignes == {
        "cycle": 1,
        "jour_traite": 30,
        "erreur_mapping": 2,
        "sans_date_publication": 3,
        "jour_vide": 4,
    }


def test_lignes_rapport_compteurs_prefixes_par_source():
    stats = Counter(
        {
            "joue:retenu": 3,
            "joue:exclu:result": 1,
            "joue:chaine_recouvrement": 2,
            "joue:inconnu": 5,
            "retenu": 4,
        }
    )
    df = pl.DataFrame(
        {"sourceDataset": ["boamp", "joue"], "formatSource": ["legacy", "eforms"]}
    )
    lignes = lignes_rapport(stats, df)
    assert {"indicateur": "joue:retenu", "valeur": 3} in lignes
    assert {"indicateur": "joue:exclu:result", "valeur": 1} in lignes
    assert {"indicateur": "joue:chaine_recouvrement", "valeur": 2} in lignes
    assert {"indicateur": "retenu", "valeur": 4} in lignes
    assert not any(ligne["indicateur"] == "joue:inconnu" for ligne in lignes)
    indicateurs = {ligne["indicateur"] for ligne in lignes}
    assert "sourceDataset:joue" in indicateurs
    df = pl.DataFrame(
        {
            "sourceDataset": ["boamp", "joue"],
            "formatSource": ["legacy", "eforms"],
            "objet": ["a", None],
        }
    )
    indicateurs = {ligne["indicateur"] for ligne in lignes_rapport(Counter(), df)}
    assert "remplissage:objet:joue/eforms" in indicateurs
    assert "remplissage:objet:boamp/legacy" in indicateurs

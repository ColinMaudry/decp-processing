from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.tasks.scrap.deao.aws.parse import (
    aucun_resultat,
    badges,
    date_courte,
    date_heure_courte,
    departements,
    numeros_avis,
    parse_avis,
    parse_liste,
    trop_de_resultats,
)

DOSSIER = Path(__file__).parent / "data" / "aws"
PARIS = ZoneInfo("Europe/Paris")


def lire(nom: str) -> str:
    return (DOSSIER / nom).read_text(encoding="utf-8")


def test_trop_de_resultats():
    assert trop_de_resultats(lire("liste_trop_large.html")) == 4093
    assert trop_de_resultats(lire("liste_25.html")) is None


def test_badges_et_liste_par_onglet():
    html = lire("liste_13_S_p1.html")
    assert badges(html)["s"] > 10 and badges(html)["t"] == 0
    avis = parse_liste(html, "s")
    assert len(avis) == 10 and all(a.nature == "Services" for a in avis)
    assert all(
        a.idm.isdigit() and a.chemin_avis.startswith("/Annonces/MPI-pub-") for a in avis
    )
    assert parse_liste(html, "t") == []


def test_liste_tous_onglets_et_natures():
    avis = parse_liste(lire("liste_25.html"))
    # 27 blocs dans la page, dont 5 sans aucun lien IDM (ignorés)
    assert len(avis) == 22
    assert {a.nature for a in avis} <= {"Travaux", "Services", "Fournitures"}
    assert len({a.nature for a in avis}) == 3
    assert len(parse_liste(lire("liste_25.html"), "f")) < 22


def test_nom_sans_code_postal_et_entites():
    a = parse_liste(lire("liste_25.html"))[0]
    assert "(" not in a.acheteur_nom and "&#039;" not in a.acheteur_nom
    assert a.code_postal is not None and len(a.code_postal) == 5
    r = parse_liste(lire("liste_13_S_p1.html"), "s")[0]
    assert r.acheteur_nom == "Région Provence-Alpes-Côte d'Azur"
    assert r.code_postal == "13481"


def test_bloc_complet():
    par_chemin = {a.chemin_avis: a for a in parse_liste(lire("liste_25.html"))}
    a = par_chemin["/Annonces/MPI-pub-2026314252.htm"]
    assert a.idm.isdigit()
    assert a.numero_avis == "2026314252"
    assert a.acheteur_nom == "MAIRIE D'AUDINCOURT"
    assert a.code_postal == "25400"
    assert a.reference == "202641"
    assert a.objet.startswith("MARCHE 2026_41 TRAVAUX")
    assert a.nombre_lots == 3
    assert a.date_publication == date(2026, 11, 10)
    assert a.date_limite == datetime(2026, 12, 2, 12, 0, tzinfo=PARIS)
    assert a.nature == "Travaux"
    assert a.considerations_environnementales is True
    assert a.considerations_sociales is False


def test_pictogramme_social():
    par_chemin = {a.chemin_avis: a for a in parse_liste(lire("liste_13_S_p1.html"))}
    a = par_chemin["/Annonces/MPI-pub-2026260690.htm"]
    assert a.considerations_sociales is True
    assert a.considerations_environnementales is True
    assert a.nombre_lots == 4
    s = par_chemin["/Annonces/MPI-pub-2026287866.htm"]
    assert s.considerations_sociales is True and s.nombre_lots == 0


def test_bloc_sans_idm_ignore():
    chemins = {a.chemin_avis for a in parse_liste(lire("liste_25.html"))}
    assert "/Annonces/MPI-pub-2026275758.htm" not in chemins


def test_dates_courtes():
    assert date_courte("10/11/26") == date(2026, 11, 10)
    assert date_heure_courte("02/12/26 à 12h00") == datetime(
        2026, 12, 2, 12, 0, tzinfo=PARIS
    )
    assert date_heure_courte("") is None and date_heure_courte("à préciser") is None


def test_departements():
    d = departements(lire("rechercher.html"))
    assert {"1", "13", "75", "201", "202"} <= set(d)
    assert "X" not in d and not any("," in x for x in d) and len(d) >= 96


def test_avis_alloti():
    a = parse_avis(lire("avis_2026314252.html"))
    assert a.siret == "21250031800010" and len(a.siret) == 14
    assert a.lots and len(a.lots) == 3 and all(lot.code_cpv for lot in a.lots)
    assert a.lots[0].numero == "01" and a.lots[0].libelle == "Voiries"
    assert a.lots[0].description == "Voiries"
    assert a.lots[0].lieu == "Espaces publics 25400 AUDINCOURT"
    assert a.lots[0].code_postal_lieu == "25400"
    assert a.lots[0].montant_estime is None
    assert a.lots[0].code_cpv == "44113320"
    assert a.date_limite == datetime(2026, 12, 2, 12, 0, tzinfo=PARIS)
    assert a.cpv_principal == "45233141"
    assert a.date_envoi == date(2026, 11, 10)
    assert a.acheteur_nom == "MAIRIE D'AUDINCOURT"
    assert a.adresse_voie == "8 Avenue Aristide Briand"
    assert a.adresse_cp == "25400" and a.adresse_ville == "AUDINCOURT"
    assert a.objet.startswith("MARCHE 2026_41")
    assert a.reference == "202641"
    assert a.type_marche == "Travaux"
    assert a.mode == "Procédure adaptée ouverte"
    assert a.technique == "Accord-Cadre"
    assert a.nuts == "FRC21"
    assert a.duree_mois == 24
    assert a.description.startswith("L'accord-cadre sera exécuté")


def test_avis_non_alloti():
    a = parse_avis(lire("avis_2026275758.html"))
    assert a.lots == []
    assert a.siret == "24250036100090"
    assert a.type_marche == "Services" and a.mode == "Procédure ouverte"
    assert a.cpv_principal == "65130000"
    assert a.duree_mois == 48
    # avis rectifié : la date limite lue est la nouvelle (« Remise des offres le »)
    assert a.date_limite == datetime(2026, 11, 16, 12, 0, tzinfo=PARIS)
    assert a.date_envoi == date(2026, 9, 30)


def test_avis_booleens_lus():
    a = parse_avis(lire("avis_2026314252.html"))
    assert a.marche_reserve is False
    assert a.groupement is False
    assert a.variantes is None  # « variantes exigées : Non » ne dit pas le contraire
    n = parse_avis(lire("avis_2026275758.html"))
    assert n.marche_reserve is False and n.groupement is None
    assert n.variantes == "interdites"  # « Les variantes sont refusées »
    # variantes déclarées par lot seulement
    assert parse_avis(lire("avis_2026260690.html")).variantes == "interdites"
    # considérations : l'avis ne le dit pas explicitement dans ces fixtures
    assert a.considerations_sociales is None
    assert a.considerations_environnementales is None


@pytest.mark.parametrize(
    "avant, apres, champ, attendu",
    [
        ("<b>Marché réservé :</b>\n\t\t\t\t\t\t\t\tNon", "<b>Marché réservé :</b> Oui",
         "marche_reserve", True),
        ("Groupement de commandes : Non", "Groupement de commandes : Oui",
         "groupement", True),
        ("Les variantes sont exigées : Non", "Les variantes sont exigées : Oui",
         "variantes", "exigées"),
        ("Les variantes sont exigées : Non", "Les variantes sont autorisées",
         "variantes", "autorisées"),
        ("Les variantes sont exigées : Non", "Les variantes sont autorisées : Non",
         "variantes", "interdites"),
        ("Groupement de commandes : Non",
         "Groupement de commandes : Non Considérations sociales : Oui",
         "considerations_sociales", True),
    ],
)  # fmt: skip
def test_avis_formulations_oui(avant, apres, champ, attendu):
    html = lire("avis_2026314252.html")
    assert avant in html
    assert getattr(parse_avis(html.replace(avant, apres)), champ) == attendu


def test_avis_sans_objet_ni_acheteur():
    with pytest.raises(ValueError):
        parse_avis("<html><body><p>Rien</p></body></html>")


def test_date_publication_impossible_ignore_le_bloc():
    html = lire("liste_25.html")
    assert "Publié le\n                            10/11/26" in html
    modifie = html.replace(
        "Publié le\n                            10/11/26",
        "Publié le\n                            31/02/26",
        1,
    )
    avis = parse_liste(modifie)
    chemins = {a.chemin_avis for a in avis}
    assert len(avis) == 21
    assert "/Annonces/MPI-pub-2026314252.htm" not in chemins


def test_date_envoi_impossible_donne_none():
    html = lire("avis_2026314252.html")
    assert "Envoi le 10/11/26" in html
    assert (
        parse_avis(html.replace("Envoi le 10/11/26", "Envoi le 31/02/26")).date_envoi
        is None
    )


def test_numeros_avis_compte_tous_les_blocs():
    html = lire("liste_25.html")
    numeros = numeros_avis(html)
    # 27 blocs, dont les 5 sans IDM
    assert len(numeros) == 27 and len(parse_liste(html)) == 22
    assert all(n.isdigit() for n in numeros)
    assert {a.numero_avis for a in parse_liste(html)} <= set(numeros)
    assert len(numeros_avis(html, "f")) < 27
    assert len(numeros_avis(lire("liste_13_S_p1.html"), "s")) == 10


def test_aucun_resultat():
    assert aucun_resultat(lire("aucun_resultat.html"))
    assert not aucun_resultat(lire("liste_25.html"))
    assert not aucun_resultat(lire("rechercher.html"))
    assert aucun_resultat("<p>Aucun&nbsp;r&eacute;sultat\n trouv&eacute;.</p>")

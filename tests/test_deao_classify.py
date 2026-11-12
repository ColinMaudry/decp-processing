from pathlib import Path

import pytest
from lxml import etree

from src.tasks.scrap.deao.boamp_joue.boamp.classify import classify
from src.tasks.scrap.deao.boamp_joue.boamp.xml import (
    annonce_anterieure,
    idweb,
    indexation_choice,
)

FIXTURES = Path(__file__).parent / "data" / "deao"


def load(nom):
    return etree.parse(FIXTURES / nom).getroot()


@pytest.mark.parametrize(
    "fichier, retenu, type_avis, pre, fmt",
    [
        ("26-87868.xml", True, "initial", False, "eforms"),
        ("26-99074.xml", True, "rectificatif", None, "eforms"),
        ("26-99891.xml", True, "rectificatif", None, "legacy"),
        ("26-99542.xml", True, "annulation", None, "legacy"),
        ("26-99870.xml", True, "initial", False, "legacy"),
        ("26-98723.xml", True, "initial", False, "eforms"),  # PIN valant MEC
        ("26-97189.xml", True, "initial", True, "eforms"),  # PIN simple
        ("26-94806.xml", True, "initial", True, "eforms"),  # PIN PROFIL_ACHETEUR
        ("26-98989.xml", False, None, None, "eforms"),  # MODIFICATION
        ("26-99072.xml", False, None, None, "eforms"),  # ATTRIBUTION
    ],
)
def test_classify(fichier, retenu, type_avis, pre, fmt):
    c = classify(load(fichier))
    assert c.retenu is retenu
    assert c.typeAvis == type_avis
    assert c.preInformation is pre
    assert c.formatSource == fmt


def test_classify_nature_exclue_est_renseignee():
    assert classify(load("26-98989.xml")).nature == "MODIFICATION"


def test_helpers_gestion():
    root = load("26-99074.xml")
    assert idweb(root) == "26-99074" and annonce_anterieure(root) == "26-87868"
    assert indexation_choice(root, "TYPE_PROCEDURE") == "OUVERT"

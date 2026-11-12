import csv
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.config import BASE_DIR
from src.tasks.deao.codes import PROCEDURE_INDEXATION, libelle
from src.tasks.deao.values import (
    departement_depuis_cp,
    departement_depuis_nuts,
    duree_mois,
    normalize_lot_id,
    parse_datetime_paris,
)

PARIS = ZoneInfo("Europe/Paris")


@pytest.mark.parametrize(
    "valeur, heure, attendu",
    [
        (
            "2026-10-22T12:00:00.000+02:00",
            None,
            datetime(2026, 10, 22, 12, 0, tzinfo=PARIS),
        ),
        ("2026-11-16T16:00:00Z", None, datetime(2026, 11, 16, 17, 0, tzinfo=PARIS)),
        ("2026-03-30T22:00:00", None, datetime(2026, 3, 30, 22, 0, tzinfo=PARIS)),
        (
            "2026-10-30+01:00",
            "16:30:00+01:00",
            datetime(2026, 10, 30, 16, 30, tzinfo=PARIS),
        ),
        ("2026-10-30+01:00", None, datetime(2026, 10, 30, 23, 59, 59, tzinfo=PARIS)),
        ("pas une date", None, None),
        (None, None, None),
    ],
)
def test_parse_datetime_paris(valeur, heure, attendu):
    resultat = parse_datetime_paris(valeur, heure)
    assert resultat == attendu
    if attendu is not None:
        assert resultat.utcoffset() == attendu.utcoffset()
        assert resultat.tzinfo == PARIS


@pytest.mark.parametrize(
    "v, u, attendu",
    [
        ("45", "DAY", 2),
        ("30", "DAY", 1),
        ("2", "YEAR", 24),
        ("29", "mois", 29),
        ("1.5", "MONTH", 2),
        ("3", "WEEK", None),
        (None, "MONTH", None),
        ("NaN", "MONTH", None),
        ("inf", "MONTH", None),
        ("-3", "MONTH", None),
        ("1e30", "YEAR", None),
        ("1e30", "MONTH", None),
        ("1200", "MONTH", 1200),
        ("101", "YEAR", None),
    ],
)
def test_duree_mois(v, u, attendu):
    assert duree_mois(v, u) == attendu


@pytest.mark.parametrize(
    "raw, attendu",
    [
        ("LOT-0001", "1"),
        ("LOT-0000", "0"),
        ("PAR-0001", "1"),
        ("03", "3"),
        ("A", "A"),
        (None, None),
    ],
)
def test_normalize_lot_id(raw, attendu):
    assert normalize_lot_id(raw) == attendu


@pytest.mark.parametrize(
    "cp, dep",
    [
        ("75004", "75"),
        ("29260", "29"),
        ("20000", "2A"),
        ("20137", "2A"),
        ("20250", "2B"),
        ("20600", "2B"),
        ("97110", "971"),
        ("98800", "988"),
        (None, None),
    ],
)
def test_departement_depuis_cp(cp, dep):
    assert departement_depuis_cp(cp) == dep


@pytest.mark.parametrize(
    "nuts, dep",
    [
        ("FRK27", "73"),
        ("FR101", "75"),
        ("FRM01", "2A"),
        ("FRM02", "2B"),
        ("FRY10", "971"),
        ("FRY50", "976"),
        ("FRB03", "36"),
        ("FRC24", "90"),
        ("FRE21", "02"),
        ("FRH02", "29"),
        ("FRI15", "64"),
        ("FRJ23", "31"),
        ("FRL04", "13"),
        ("XX999", None),
        (None, None),
    ],
)
def test_departement_depuis_nuts(nuts, dep):
    assert departement_depuis_nuts(nuts) == dep


def test_table_nuts3_complete():
    with open(BASE_DIR / "reference" / "nuts3_departements.csv") as f:
        lignes = list(csv.DictReader(f))
    assert len(lignes) == 101
    assert len({r["nuts3"] for r in lignes}) == 101
    assert len({r["departement_code"] for r in lignes}) == 101


def test_libelle_non_mappe_compte():
    c = Counter()
    assert libelle(PROCEDURE_INDEXATION, "INCONNU", c, "procedure") == "INCONNU"
    assert c["non_mappe:procedure:INCONNU"] == 1
    assert (
        libelle(PROCEDURE_INDEXATION, "OUVERT", c, "procedure")
        == "Appel d'offres ouvert"
    )
    assert libelle(PROCEDURE_INDEXATION, None, c, "procedure") is None

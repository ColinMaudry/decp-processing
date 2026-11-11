import pytest

from src.tasks.utils import siret_valide


@pytest.mark.parametrize(
    "s, attendu",
    [
        ("83942912300020", True),  # Luhn OK
        ("32384135300911", True),
        ("35600000000010", True),  # La Poste : Luhn KO mais somme % 5 == 0
        ("35600000000011", False),  # La Poste, somme % 5 != 0
        ("12345678900001", False),  # Luhn KO
        ("8394291230002", False),  # 13 chiffres
        ("8394291230002A", False),
        ("", False),
        (None, False),
    ],
)
def test_siret_valide(s, attendu):
    assert siret_valide(s) is attendu

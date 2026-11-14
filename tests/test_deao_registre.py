import pytest

from src.config import REFERENCE_DIR, charger_registre_deao


def test_registre_reference():
    r = {s.code: s for s in charger_registre_deao(REFERENCE_DIR / "deao_sources.yaml")}
    assert set(r) == {"boamp", "joue", "megalis_bretagne", "maximilien", "place"}
    assert r["boamp"].resource_id == "9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62"
    assert r["boamp"].min_rows == 50000
    assert r["joue"].resource_id == "8fd238e4-d5ab-40d1-9b61-a07c2fdb020c"
    assert r["joue"].min_rows == 10000
    assert r["boamp"].open_data and r["joue"].open_data
    assert r["boamp"].url_base is None
    assert r["megalis_bretagne"].resource_id == "c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76"
    assert r["megalis_bretagne"].min_rows == 300
    assert r["megalis_bretagne"].url_base == "https://marches.megalis.bretagne.bzh"
    assert r["maximilien"].resource_id == "e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5"
    assert r["maximilien"].min_rows == 150
    assert r["maximilien"].url_base == "https://marches.maximilien.fr"
    assert not r["megalis_bretagne"].open_data and not r["maximilien"].open_data
    assert r["place"].resource_id == "111e3919-ec45-42c4-9c65-5d8bb6a41cb0"
    assert r["place"].min_rows == 1000
    assert r["place"].url_base == "https://www.marches-publics.gouv.fr"
    assert not r["place"].open_data
    assert not any(s.skip for s in r.values())


def _ecrire(tmp_path, contenu):
    p = tmp_path / "reg.yaml"
    p.write_text(contenu)
    return p


def test_registre_champ_manquant(tmp_path):
    p = _ecrire(
        tmp_path, "- code: boamp\n  name: B\n  open_data: true\n  min_rows: 1\n"
    )
    with pytest.raises(ValueError):
        charger_registre_deao(p)


def test_registre_code_en_double(tmp_path):
    e = "- code: boamp\n  name: B\n  open_data: true\n  resource_id: x\n  min_rows: 1\n"
    with pytest.raises(ValueError):
        charger_registre_deao(_ecrire(tmp_path, e + e))


def test_registre_skip_lu(tmp_path):
    p = _ecrire(
        tmp_path,
        "- code: a\n  name: A\n  open_data: false\n  resource_id: x\n  min_rows: 1\n  skip: true\n",
    )
    (s,) = charger_registre_deao(p)
    assert s.skip is True and s.open_data is False


def test_url_base_lue(tmp_path):
    p = _ecrire(
        tmp_path,
        "- code: a\n  name: A\n  open_data: false\n  resource_id: x\n  min_rows: 1\n"
        "  url_base: https://exemple.fr\n",
    )
    (s,) = charger_registre_deao(p)
    assert s.url_base == "https://exemple.fr"


def test_url_base_non_https_refusee(tmp_path):
    p = _ecrire(
        tmp_path,
        "- code: a\n  name: A\n  open_data: false\n  resource_id: x\n  min_rows: 1\n"
        "  url_base: http://exemple.fr\n",
    )
    with pytest.raises(ValueError, match="url_base"):
        charger_registre_deao(p)

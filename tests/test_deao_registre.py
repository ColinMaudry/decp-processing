import pytest

from src.config import REFERENCE_DIR, charger_registre_deao


def test_registre_reference():
    r = {s.code: s for s in charger_registre_deao(REFERENCE_DIR / "deao_sources.yaml")}
    assert set(r) == {"boamp", "joue"}
    assert r["boamp"].resource_id == "9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62"
    assert r["boamp"].min_rows == 50000
    assert r["joue"].resource_id == "8fd238e4-d5ab-40d1-9b61-a07c2fdb020c"
    assert r["joue"].min_rows == 10000
    assert all(s.open_data and not s.skip for s in r.values())


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

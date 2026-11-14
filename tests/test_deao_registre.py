import pytest

from src.config import REFERENCE_DIR, charger_registre_deao


def test_registre_reference():
    r = {s.code: s for s in charger_registre_deao(REFERENCE_DIR / "deao_sources.yaml")}
    assert {
        "boamp",
        "joue",
        "marches.megalis.bretagne.bzh",
        "marches.maximilien.fr",
        "www.marches-publics.gouv.fr",
        "puma.cnrs.fr",
    } <= set(r)
    assert len(r) == 38
    atexo = [s for s in r.values() if s.plateforme == "atexo"]
    assert len(atexo) == 35 and not any(s.open_data for s in atexo)
    aws = r["www.marches-publics.info"]
    assert aws.plateforme == "aws" and aws.min_rows == 2000 and not aws.open_data
    assert aws.resource_id == "eb5801ee-a8d0-4419-bab2-1e5c25ca2381"
    assert len({s.resource_id for s in r.values()}) == len(r)
    assert r["boamp"].resource_id == "9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62"
    assert r["boamp"].min_rows == 50000
    assert r["joue"].resource_id == "8fd238e4-d5ab-40d1-9b61-a07c2fdb020c"
    assert r["joue"].min_rows == 10000
    assert r["boamp"].open_data and r["joue"].open_data
    assert r["boamp"].url_base is None
    assert (
        r["marches.megalis.bretagne.bzh"].resource_id
        == "c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76"
    )
    assert r["marches.megalis.bretagne.bzh"].min_rows == 300
    assert (
        r["marches.megalis.bretagne.bzh"].url_base
        == "https://marches.megalis.bretagne.bzh"
    )
    assert (
        r["marches.maximilien.fr"].resource_id == "e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5"
    )
    assert r["marches.maximilien.fr"].min_rows == 150
    assert r["marches.maximilien.fr"].url_base == "https://marches.maximilien.fr"
    assert (
        not r["marches.megalis.bretagne.bzh"].open_data
        and not r["marches.maximilien.fr"].open_data
    )
    assert (
        r["www.marches-publics.gouv.fr"].resource_id
        == "111e3919-ec45-42c4-9c65-5d8bb6a41cb0"
    )
    assert r["www.marches-publics.gouv.fr"].min_rows == 1000
    assert (
        r["www.marches-publics.gouv.fr"].url_base
        == "https://www.marches-publics.gouv.fr"
    )
    assert not r["www.marches-publics.gouv.fr"].open_data
    assert not any(s.skip for s in r.values())
    assert all(s.plateforme in ("atexo", "aws") for s in r.values() if s.url_base)


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
        "- code: exemple.fr\n  name: A\n  open_data: false\n  resource_id: x\n"
        "  min_rows: 1\n  url_base: https://exemple.fr\n",
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


_PLATEFORME = (
    "- code: {code}\n  name: A\n  open_data: false\n  resource_id: x\n  min_rows: 1\n"
)


def test_code_plateforme_egal_nom_hote(tmp_path):
    p = _ecrire(
        tmp_path,
        _PLATEFORME.format(code="marches.exemple.fr")
        + "  url_base: https://marches.exemple.fr\n  plateforme: atexo\n",
    )
    (s,) = charger_registre_deao(p)
    assert s.plateforme == "atexo"


def test_code_different_du_nom_hote_refuse(tmp_path):
    p = _ecrire(
        tmp_path,
        _PLATEFORME.format(code="exemple") + "  url_base: https://marches.exemple.fr\n",
    )
    with pytest.raises(ValueError, match="nom d'hôte"):
        charger_registre_deao(p)


def test_plateforme_inconnue_refusee(tmp_path):
    p = _ecrire(
        tmp_path,
        _PLATEFORME.format(code="marches.exemple.fr")
        + "  url_base: https://marches.exemple.fr\n  plateforme: autre\n",
    )
    with pytest.raises(ValueError, match="plateforme inconnue"):
        charger_registre_deao(p)


def test_plateforme_sans_url_base_refusee(tmp_path):
    p = _ecrire(tmp_path, _PLATEFORME.format(code="a") + "  plateforme: atexo\n")
    with pytest.raises(ValueError, match="url_base requise"):
        charger_registre_deao(p)


def test_plateforme_aws_acceptee(tmp_path):
    p = _ecrire(
        tmp_path,
        _PLATEFORME.format(code="www.marches-publics.info")
        + "  url_base: https://www.marches-publics.info\n  plateforme: aws\n",
    )
    (s,) = charger_registre_deao(p)
    assert s.plateforme == "aws"

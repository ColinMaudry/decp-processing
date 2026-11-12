from src.deployments import deao_deployment_specs, decp_deployment_specs

PRODUITS = {"attributions", "appels-offres"}
ETAPES = {"scrap", "traitement", "referentiel"}
SOURCES = {"boamp", "joue", "aws", "marches-securises", "dume", "klekoon"}
ENVS = {"prod", "dev"}


def _toutes():
    return decp_deployment_specs() + deao_deployment_specs()


def test_tags_familles():
    for s in _toutes():
        tags = set(s["tags"])
        assert tags <= PRODUITS | ETAPES | SOURCES | ENVS, s["name"]
        assert len(tags & ENVS) == 1, s["name"]
        assert len(tags & ETAPES) == 1, s["name"]
        assert tags & PRODUITS or tags & {"referentiel"}, s["name"]


def test_tags_env_selon_la_branche():
    for s in _toutes():
        branche = s.get("parameters", {}).get("branch", s["branch"])
        assert ("dev" if branche == "dev" else "prod") in s["tags"], s["name"]


def test_tags_decp():
    specs = {s["name"]: s for s in decp_deployment_specs()}
    for nom in ("decp-processing", "decp-processing-exec-prod"):
        assert set(specs[nom]["tags"]) == {"attributions", "traitement", "prod"}
    assert set(specs["decp-processing-exec-dev"]["tags"]) == {
        "attributions",
        "traitement",
        "dev",
    }
    assert set(specs["sirene-preprocess"]["tags"]) == {"referentiel", "prod"}
    assert set(specs["scrap-marches-securises"]["tags"]) == {
        "attributions",
        "scrap",
        "marches-securises",
        "prod",
    }
    assert "aws" in specs["scrap-aws"]["tags"]
    assert "dume" in specs["scrap-dume"]["tags"]
    assert not set(specs["scrap"]["tags"]) & SOURCES
    assert not set(specs["scrap-dev"]["tags"]) & SOURCES


def test_decp_specs_inchanges():
    specs = {s["name"]: s for s in decp_deployment_specs()}
    assert set(specs) == {
        "decp-processing",
        "decp-processing-exec-prod",
        "decp-processing-dev",
        "decp-processing-exec-dev",
        "sirene-preprocess",
        "sirene-preprocess-exec-prod",
        "sirene-preprocess-dev",
        "sirene-preprocess-exec-dev",
        "scrap-marches-securises",
        "scrap-aws",
        "scrap-dume",
        "scrap",
        "scrap-dev",
    }
    assert specs["decp-processing"]["cron"] == "0 5 * * *"
    assert specs["decp-processing"]["work_pool_name"] == "local"
    assert specs["decp-processing-exec-prod"]["work_pool_name"] == "scaleway-prod"

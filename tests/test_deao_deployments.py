from src.deployments import deao_deployment_specs


def test_deao_ne_partage_pas_le_dist_des_decp():
    specs = deao_deployment_specs()
    for s in specs:
        dist = s.get("parameters", {}).get("dist_dir")
        if dist is not None:
            assert dist.startswith("/srv/shared/deao/") and "/decp/" not in dist


def test_deao_lanceur_prod():
    s = next(s for s in deao_deployment_specs() if s["name"] == "deao-processing")
    assert s["parameters"]["instance_type"] == "STANDARD3-X4C-16G"
    assert s["cron"] == "0 9 * * *"


def test_deao_quatre_deploiements_references_coherentes():
    specs = {s["name"]: s for s in deao_deployment_specs()}
    assert set(specs) == {
        "deao-processing",
        "deao-processing-exec-prod",
        "deao-processing-dev",
        "deao-processing-exec-dev",
    }
    for env in ("prod", "dev"):
        launcher = specs["deao-processing" if env == "prod" else "deao-processing-dev"]
        assert (
            launcher["parameters"]["deployment"]
            == f"deao-processing/deao-processing-exec-{env}"
        )
        assert launcher["work_pool_name"] == "local"
        assert (
            specs[f"deao-processing-exec-{env}"]["work_pool_name"] == f"scaleway-{env}"
        )
    assert "cron" not in specs["deao-processing-dev"]
    assert specs["deao-processing-dev"]["parameters"]["branch"] == "dev"
    assert specs["deao-processing-exec-dev"]["branch"] == "dev"
    assert specs["deao-processing-exec-prod"]["job_variables"]["env"] == {
        "DECP_PROCESSING_PUBLISH": "True"
    }
    assert specs["deao-processing-exec-dev"]["job_variables"]["env"] == {
        "DECP_PROCESSING_PUBLISH": "False"
    }


def test_deao_lanceurs_duree_max_12h():
    """R19 : le premier crawl complet (~500k GET) dépasse les 4 h par défaut."""
    import inspect

    from src.flows.launcher import run_on_scaleway

    params = inspect.signature(run_on_scaleway.fn).parameters
    specs = {s["name"]: s for s in deao_deployment_specs()}
    for nom in ("deao-processing", "deao-processing-dev"):
        parametres = specs[nom]["parameters"]
        assert parametres["max_duration_hours"] == 12
        assert set(parametres) <= set(params)

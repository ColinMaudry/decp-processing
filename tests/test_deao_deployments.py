from src.deployments import deao_deployment_specs

NOMS = {
    "deao-scrap-open",
    "deao-scrap-closed",
    "deao-processing",
    "deao-scrap-open-dev",
    "deao-scrap-closed-dev",
    "deao-processing-dev",
}


def _specs() -> dict:
    return {s["name"]: s for s in deao_deployment_specs()}


def test_deao_noms_et_entrypoints():
    specs = _specs()
    assert set(specs) == NOMS
    for nom, s in specs.items():
        # deao-scrap-open → scrap_open ; deao-processing → deao_processing
        base = nom.removesuffix("-dev")
        if base.startswith("deao-scrap-"):
            base = base.removeprefix("deao-")
        base = base.replace("-", "_")
        assert s["entrypoint"] == f"src/flows/{base}.py:{base}"
        assert s["branch"] == ("dev" if nom.endswith("-dev") else "main")


def test_deao_crons_et_pools():
    specs = _specs()
    assert specs["deao-scrap-open"]["cron"] == "0 8,12,16 * * *"
    assert specs["deao-scrap-closed"]["cron"] == "0 6 * * *"
    assert "cron" not in specs["deao-processing"]
    for s in specs.values():
        assert s["work_pool_name"] == "local"


def test_deao_concurrence():
    specs = _specs()
    assert specs["deao-scrap-open"]["concurrency_limit"] == 1
    assert specs["deao-scrap-closed"]["concurrency_limit"] == 1


def test_deao_dev_sans_publication():
    for nom, s in _specs().items():
        if not nom.endswith("-dev"):
            assert s["job_variables"]["env"]["DECP_PROCESSING_PUBLISH"] == "True"
            assert "prod" in s["tags"] and "dev" not in s["tags"]
            continue
        assert s["branch"] == "dev"
        assert "cron" not in s
        assert s["job_variables"]["env"]["DECP_PROCESSING_PUBLISH"] == "False"
        assert "dev" in s["tags"] and "prod" not in s["tags"]


def test_deao_env_dirs():
    for nom, s in _specs().items():
        env = s["job_variables"]["env"]
        env_nom = "dev" if nom.endswith("-dev") else "prod"
        assert env["DEAO_DATA_DIR"] == f"/home/yunohost.app/prefect/deao/{env_nom}"
        assert env["DEAO_DIST_DIR"] == f"/srv/shared/deao/{env_nom}/dist"


def test_deao_tags():
    specs = _specs()
    assert set(specs["deao-scrap-open"]["tags"]) == {
        "appels-offres",
        "scrap",
        "boamp",
        "joue",
        "prod",
    }
    assert set(specs["deao-scrap-closed"]["tags"]) == {"appels-offres", "scrap", "prod"}
    assert set(specs["deao-processing-dev"]["tags"]) == {
        "appels-offres",
        "traitement",
        "dev",
    }


def test_plus_de_scaleway_pour_deao():
    for s in deao_deployment_specs():
        assert "launcher" not in s["entrypoint"]
        assert not s["work_pool_name"].startswith("scaleway")
        assert "parameters" not in s


def test_deao_data_dir_distinct_par_environnement():
    specs = _specs()
    for nom in ("deao-scrap-open", "deao-scrap-closed", "deao-processing"):
        prod = specs[nom]["job_variables"]["env"]["DEAO_DATA_DIR"]
        dev = specs[f"{nom}-dev"]["job_variables"]["env"]["DEAO_DATA_DIR"]
        assert prod != dev

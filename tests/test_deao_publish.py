from pathlib import Path

import pytest

from src import config
from src.tasks import publish


@pytest.mark.parametrize(
    "pub, dataset, n, attendu",
    [
        (True, "ds", 50000, True),
        (True, "ds", 49999, False),
        (False, "ds", 10**6, False),
        (True, "", 10**6, False),
    ],
)
def test_should_publish_deao(monkeypatch, pub, dataset, n, attendu):
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", pub)
    monkeypatch.setattr(config, "DEAO_DATASET_ID", dataset)
    monkeypatch.setattr(config, "DEAO_MIN_ROWS_TO_PUBLISH", 50000)
    assert publish.should_publish_deao(n) is attendu


def test_publish_deao_appelle_update_resource(monkeypatch, tmp_path):
    appels = []
    monkeypatch.setattr(
        publish, "update_resource", lambda *a: appels.append(a) or {"success": True}
    )
    monkeypatch.setattr(config, "DEAO_DATASET_ID", "ds")
    monkeypatch.setattr(config, "DEAO_PARQUET_RESOURCE_ID", "p")
    monkeypatch.setattr(config, "DEAO_CSV_RESOURCE_ID", "c")
    monkeypatch.setattr(config, "DEAO_SCHEMA_RESOURCE_ID", "s")
    monkeypatch.setattr(config, "DATAGOUVFR_API_KEY", "k")
    publish.publish_deao_to_datagouv(tmp_path)
    assert appels == [
        ("ds", "p", str(tmp_path / "deao.parquet"), "k"),
        ("ds", "c", str(tmp_path / "deao.csv.gz"), "k"),
        ("ds", "s", str(tmp_path / "schema.json"), "k"),
    ]


def test_publish_deao_ignore_ressource_sans_id(monkeypatch, tmp_path):
    appels = []
    monkeypatch.setattr(
        publish, "update_resource", lambda *a: appels.append(a) or {"success": True}
    )
    monkeypatch.setattr(config, "DEAO_DATASET_ID", "ds")
    monkeypatch.setattr(config, "DEAO_PARQUET_RESOURCE_ID", "p")
    monkeypatch.setattr(config, "DEAO_CSV_RESOURCE_ID", "")
    monkeypatch.setattr(config, "DEAO_SCHEMA_RESOURCE_ID", "")
    publish.publish_deao_to_datagouv(Path(tmp_path))
    assert len(appels) == 1


def test_publish_deao_echec_leve_apres_toutes_les_tentatives(monkeypatch, tmp_path):
    appels = []

    def stub(dataset, resource_id, file, key):
        appels.append(resource_id)
        return {"success": resource_id != "p"}

    monkeypatch.setattr(publish, "update_resource", stub)
    monkeypatch.setattr(config, "DEAO_DATASET_ID", "ds")
    monkeypatch.setattr(config, "DEAO_PARQUET_RESOURCE_ID", "p")
    monkeypatch.setattr(config, "DEAO_CSV_RESOURCE_ID", "c")
    monkeypatch.setattr(config, "DEAO_SCHEMA_RESOURCE_ID", "s")
    with pytest.raises(RuntimeError, match="deao.parquet"):
        publish.publish_deao_to_datagouv(tmp_path)
    assert appels == ["p", "c", "s"]

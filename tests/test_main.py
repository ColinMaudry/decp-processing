import os

import pytest
from prefect.testing.utilities import prefect_test_harness

from src.flows.decp_processing import decp_processing


@pytest.fixture(autouse=False, scope="function")
def prefect_test_fixture(tmp_path_factory):
    os.environ["PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS"] = "90"

    with prefect_test_harness():
        yield

    # Force cleanup: try to stop any lingering Prefect server
    # (This is a workaround — Prefect doesn't expose a clean stop API in test harness)
    # Généré par Euria, la LLM d'Infomaniak.

    # Try to kill any remaining prefect server process
    # try:
    #     # This is a bit brute-force, but works if no other prefect server is running
    #     subprocess.run(
    #         ["pkill", "-f", "prefect server start"],
    #         stdout=subprocess.DEVNULL,
    #         stderr=subprocess.DEVNULL,
    #     )
    #     time.sleep(0.5)  # Give time to terminate
    # except Exception:
    #     pass


def test_decp_processing(prefect_test_fixture):
    decp_processing()

    import polars as pl

    from src.config import DIST_DIR

    lf_out = pl.scan_parquet(DIST_DIR / "decp.parquet")
    cols = lf_out.collect_schema().names()
    assert "montant_rationalise" in cols
    assert "montant_anomalie" in cols
    assert "montant_anomalie_raison" in cols

from src.flows.decp_processing import decp_processing
from tests.fixtures import prefect_test_harness


class TestFlow:
    def test_decp_processing(self):
        with prefect_test_harness(server_startup_timeout=30):
            decp_processing()

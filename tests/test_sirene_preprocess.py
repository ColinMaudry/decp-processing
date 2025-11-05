from flows.sirene_preprocess import sirene_preprocess
from tests.fixtures import prefect_test_harness


class TestFlow:
    def test_sirene_preprocess(self):
        with prefect_test_harness(server_startup_timeout=10):
            sirene_preprocess()

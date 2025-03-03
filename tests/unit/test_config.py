from pathlib import Path

from harness_runner.config import TestProcedureConfig


def test_from_yamlfile():
    path = Path("config/test_procedure.yaml")
    print(TestProcedureConfig.from_yamlfile(path=path))

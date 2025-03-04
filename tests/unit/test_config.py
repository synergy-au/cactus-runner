from pathlib import Path

import pytest

from harness_runner.config import TestProcedureConfig, TestProcedureDefinitionError


def test_from_yamlfile():
    """This test confirms the standard test procedure yaml file (intended for production use)
    can be read and converted to the appropriate dataclasses.
    """
    path = Path("config/test_procedure.yaml")
    test_procedures = TestProcedureConfig.from_yamlfile(path=path)
    test_procedures.validate()


@pytest.mark.parametrize(
    "filename",
    [
        "tests/data/config_with_errors1.yaml",  # No 'listeners' parameters defined for enable-listeners action (NOT-A-VALID-PARAMETER-NAME supplied instead)
        "tests/data/config_with_errors2.yaml",  # Action refers to step "NOT-A-VALID-STEP"
    ],
)
def test_TestProcedures_validate_raises_exception(filename: str):

    with pytest.raises(TestProcedureDefinitionError):
        test_procedures = TestProcedureConfig.from_yamlfile(path=Path(filename))

from harness_runner import runner
import pytest


def test_apply_db_precondition_raises_exception():
    with pytest.raises(runner.UnableToApplyDatabasePrecondition):
        runner.apply_db_precondition(precondition="path-that-does-not-exist/file-that-does-not-exist.sql")

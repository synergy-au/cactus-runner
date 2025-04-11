import pytest

from cactus_runner.app import precondition


def test_apply_db_precondition_raises_exception():
    with pytest.raises(precondition.UnableToApplyDatabasePrecondition):
        precondition.apply_db_precondition(precondition="path-that-does-not-exist/file-that-does-not-exist.sql")

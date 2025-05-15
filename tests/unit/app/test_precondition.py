import pytest
from sqlalchemy import Connection

from cactus_runner.app import precondition


def test_apply_db_precondition_raises_exception_if_path_dne(mocker):

    # We don't need to test the behavior of the DB connection
    open_connection_mock = mocker.patch("cactus_runner.app.precondition.open_connection")
    open_connection_mock.return_value = mocker.MagicMock(Connection)

    with pytest.raises(precondition.UnableToApplyDatabasePrecondition):
        precondition.apply_db_precondition(precondition="path-that-does-not-exist/file-that-does-not-exist.sql")

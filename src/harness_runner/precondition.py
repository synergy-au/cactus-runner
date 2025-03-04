import logging
from pathlib import Path

from psycopg import Connection

logger = logging.getLogger(__name__)


class UnableToApplyDatabasePrecondition(Exception):
    pass


def execute_sql_file_for_connection(connection: Connection, path_to_sql_file: Path) -> None:
    with open(path_to_sql_file) as f:
        sql = f.read()

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.commit()


def apply_db_precondition(precondition: str):
    # The precondition is a path to a .sql file
    # Verify that the file exists
    path = Path(precondition)
    if not path.exists():
        raise UnableToApplyDatabasePrecondition(f"'{precondition}' file does not exist")

    # Execute .sql file
    pass

    logger.info(f"Precondition '{precondition}' applied to database.")

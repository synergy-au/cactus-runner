import logging
from datetime import datetime, timedelta
from importlib import resources
from pathlib import Path
from zoneinfo import ZoneInfo

from envoy.server.model.aggregator import Aggregator, AggregatorCertificateAssignment
from envoy.server.model.base import Certificate
from psycopg import Connection
from sqlalchemy import insert

from cactus_runner.app.database import begin_session, open_connection

logger = logging.getLogger(__name__)


class UnableToApplyDatabasePrecondition(Exception):
    pass


def execute_sql_file_for_connection(connection: Connection, path_to_sql_file: Path) -> None:
    with open(path_to_sql_file) as f:
        sql = f.read()

    with connection.cursor() as cursor:
        cursor.execute(sql)
        connection.commit()


def apply_db_precondition(precondition: str) -> None:

    # Open connection to database
    with open_connection() as connection:
        # The precondition is either a path to a .sql file
        # or a resource made available through the cactus_test_defintions package
        path = Path(precondition)
        if path.exists():
            execute_sql_file_for_connection(connection=connection, path_to_sql_file=path)
            logger.info(f"Precondition '{precondition}' applied to database.")
        else:
            # Try access the precondition as a resource
            resource = resources.files("cactus_test_definitions") / precondition
            with resources.as_file(resource) as path:
                # Verify that the file exists
                if not path.exists():
                    raise UnableToApplyDatabasePrecondition(f"'{precondition}' file does not exist")

                execute_sql_file_for_connection(connection=connection, path_to_sql_file=path)
                logger.info(f"Precondition '{precondition}' applied to database.")


def register_aggregator(lfdi: str) -> None:
    with begin_session() as session:
        now = datetime.now(tz=ZoneInfo("UTC"))
        expiry = now + timedelta(hours=24)
        certificate = Certificate(lfdi=lfdi, created=now, expiry=expiry)
        aggregator = Aggregator(name="Cactus", created_time=now, changed_time=now)

        session.add(aggregator)
        session.add(certificate)

        session.execute(
            insert(Aggregator).values(name="NULL AGGREGATOR", created_time=now, changed_time=now, aggregator_id=0)
        )
        session.flush()

        certificate_assignment = AggregatorCertificateAssignment(
            certificate_id=certificate.certificate_id, aggregator_id=aggregator.aggregator_id
        )

        session.add(certificate_assignment)
        session.commit()

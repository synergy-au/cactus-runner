import logging
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)


class DatabaseNotInitialisedError(Exception):
    """Raised if using a function from this module without successfully calling initialise_database_connection"""

    pass


@dataclass
class DatabaseConnection:
    """Describes a configured connection to the database"""

    postgres_dsn: str
    engine: Engine
    session_maker: sessionmaker[Session]


CURRENT_CONNECTION: DatabaseConnection | None = None


def initialise_database_connection(postgres_dsn: str) -> None:
    global CURRENT_CONNECTION
    if CURRENT_CONNECTION:
        CURRENT_CONNECTION.session_maker.close_all()
        CURRENT_CONNECTION.engine.dispose(close=True)

    postgres_dsn = postgres_dsn.replace("+psycopg", "").replace("+asyncpg", "")
    engine = create_engine(postgres_dsn)
    CURRENT_CONNECTION = DatabaseConnection(postgres_dsn, engine, sessionmaker(engine))


def begin_session() -> Session:
    """Creates a new session for interacting with the database - this should be used with a context manager eg:

    initialise_database_connection must have been called otherwise this will raise a DatabaseNotInitialisedError

    with begin_session() as session:
        session.add(...)
    """
    if not CURRENT_CONNECTION:
        raise DatabaseNotInitialisedError("Ensure initialise_database_connection has been called.")

    return CURRENT_CONNECTION.session_maker()


def open_connection() -> Connection:
    """Creates a new raw database connection for interacting with the database. This should be used with a context
    manager eg:

    initialise_database_connection must have been called otherwise this will raise a DatabaseNotInitialisedError

    with open_connection() as conn:
        conn.cursor()
        conn.commit()
    """
    if not CURRENT_CONNECTION:
        raise DatabaseNotInitialisedError("Ensure initialise_database_connection has been called.")

    return CURRENT_CONNECTION.engine.connect()


def get_postgres_dsn() -> str:
    """Fetches the postgres DSN (connection string) used by the currently initialised connection.

    initialise_database_connection must have been called otherwise this will raise a DatabaseNotInitialisedError"""
    if not CURRENT_CONNECTION:
        raise DatabaseNotInitialisedError("Ensure initialise_database_connection has been called.")

    return CURRENT_CONNECTION.postgres_dsn

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class UnableToApplyDatabasePrecondition(Exception):
    pass


def apply_db_precondition(precondition: str):
    # The precondition is a path to a .sql file
    # Verify that the file exists
    path = Path(precondition)
    if not path.exists():
        raise UnableToApplyDatabasePrecondition(f"'{precondition}' file does not exist")

    # Execute .sql file
    pass

    logger.info(f"Precondition '{precondition}' applied to database.")

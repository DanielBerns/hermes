import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, Session

from hermes.domain.models import Base

logger = logging.getLogger(__name__)

class DatabaseException(Exception):
    """Custom exception for database-related errors."""
    pass

@contextmanager
def get_session(database_uri: str) -> Generator[Session, None, None]:
    """
    Provides a transactional scope around a series of database operations.

    Args:
        database_uri (str): The connection string for the database.
                            Use ':memory:' for an in-memory SQLite database.

    Yields:
        Session: The SQLAlchemy session object.
    """
    # Use an absolute path for file-based databases unless it's in-memory
    if database_uri != ":memory:":
        db_path = Path(database_uri).resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        is_new_db = not db_path.exists()
        connection_string = f"sqlite+pysqlite:///{db_path}"
    else:
        is_new_db = True
        connection_string = "sqlite+pysqlite:///:memory:"

    engine = sa.create_engine(connection_string, future=True)

    if is_new_db:
        # Create all tables if the database file is new.
        Base.metadata.create_all(engine)
        logger.info(f"New database created and schema applied at {database_uri}")

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    try:
        yield session
        session.commit()
    except Exception as e:
        logger.error(f"Database transaction failed. Rolling back. Error: {e}", exc_info=True)
        session.rollback()
        raise DatabaseException("Transaction failed and was rolled back.") from e
    finally:
        session.close()


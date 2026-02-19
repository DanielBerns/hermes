import functools
import logging
from typing import Generator

from sqlalchemy.orm import Session

from hermes.core.config import config
from hermes.core.helpers import get_resource
from hermes.core.infra import Infra
from hermes.domain.sample import Sample
from hermes.domain.database_session import get_database_session

logger = logging.getLogger(__name__)

@functools.lru_cache()
def get_infra() -> Infra:
    """
    Creates and caches the Infra instance.
    Uses defaults suitable for the dashboard execution context.
    """
    # We use a generic script name and project identifier for the dashboard
    script = "hermes_dashboard"
    project_identifier = "hermes" 
    logger.info(f"Initializing Infra for {script}...")
    return Infra(script, project_identifier)

def get_db_uri() -> str:
    """
    Resolves the database URI using the Infra instance.
    """
    infra = get_infra()
    mecon_container = infra.info_storage.container(Sample.MECON)
    db_container = infra.info_storage.container(Sample.DATABASE, base=mecon_container)
    db_uri = str(get_resource(db_container, infra.database_name, ".db"))
    logger.info(db_uri)
    return db_uri

def get_db() -> Generator[Session, None, None]:
    """
    Dependency that yields a database session.
    """
    db_uri = get_db_uri()
    with get_database_session(db_uri) as session:
        yield session

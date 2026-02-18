import logging
from typing import Any

from hermes.domain.database_repository import DatabaseRepository
from hermes.domain.database_session import get_database_session
from hermes.precios_claros.context import get_precios_claros_context
from hermes.core.storage import Storage

logger = logging.getLogger(__name__)

class DatabaseInsert:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting to process samples and inserting them in the database...")
        ctx = get_precios_claros_context(info_storage)
        
        logger.info(f"Connecting to database: {ctx.db_uri}")
        with get_database_session(ctx.db_uri) as session:
            repo = DatabaseRepository(session)
            for store in ctx.tree_store.iterate():
                try:
                    logger.info(f"Processing store {store.key} with timestamp {store.timestamp}...")
                    repo.process_sample(store)
                except Exception as e:
                    logger.error(f"Failed to process store {store.key}: {e}", exc_info=True)

        logger.info("Database update process complete.")

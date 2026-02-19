import logging
from typing import Any

from hermes.domain.database_session import get_database_session
from hermes.domain.database_repository import DatabaseRepository
from hermes.domain.article_tagging_service import ArticleTaggingService
from hermes.precios_claros.context import get_precios_claros_context
from hermes.core.storage import Storage

logger = logging.getLogger(__name__)

class DatabaseCleanArticleDescription:
    """Action to create canonical tags for article descriptions."""
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting article description tagging...")
        ctx = get_precios_claros_context(info_storage)
        
        logger.info(f"Connecting to database: {ctx.db_uri}")

        with get_database_session(ctx.db_uri) as session:
            service = ArticleTaggingService(session)
            service.generate_high_confidence_tags()
            service.clean_and_match_remaining()
            service.process_rogue_cards()
            
        logger.info("Article description tagging complete.")

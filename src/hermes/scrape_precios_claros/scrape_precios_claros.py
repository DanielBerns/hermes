import logging
from typing import Any

from hermes.domain.sample import Sample
from hermes.domain.sample_writer import SampleWriter
from hermes.domain.data_processor import DataProcessor
from hermes.domain.session import get_session
from hermes.domain.database_repository import DatabaseRepository
from hermes.domain.article_tagging_service import ArticleTaggingService
from hermes.scrape_precios_claros.sample_builder import SampleBuilder
from hermes.scrape_precios_claros.web_client import WebClient
from hermes.scrape_precios_claros.scraper import Scraper
from hermes.scrape_precios_claros.scrape_stats import get_scrape_stats
from hermes.scrape_precios_claros.context import get_precios_claros_context
from hermes.core.formatter import JSONFormatter
from hermes.core.helpers import get_directory, get_timestamp
from hermes.core.rows_selector import RowsSelector
from hermes.core.rows_writer import RowsWriter
from hermes.core.storage import Storage

logger = logging.getLogger(__name__)

class PreciosClarosException(Exception):
    pass


class PreciosClarosStart:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Initializing Precios Claros parameters...")
        ctx = get_precios_claros_context(info_storage)
        
        RowsSelector.create(
            ctx.tree_store.parameters, Sample.STATES_AND_CITIES_SELECTOR, Sample.STATES_AND_CITIES
        )

        logger.info("Initialization complete.")


class PreciosClarosUpdate:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting Precios Claros data update scrape...")
        ctx = get_precios_claros_context(info_storage)

        # Instantiate all the refactored components
        web_client = WebClient()
        scraper = Scraper(web_client)
        data_processor = DataProcessor()
        selector = RowsSelector.read(ctx.tree_store.parameters, Sample.STATES_AND_CITIES_SELECTOR)

        sample_builder = SampleBuilder(scraper, data_processor, selector)

        # Create a new store for this run
        store = ctx.tree_store.create_store()
        logger.info(f"Created new data store: {store.key}")

        writer = RowsWriter(store.home)
        formatter = JSONFormatter()
        sample_writer = SampleWriter(sample_builder, writer, formatter)

        # Execute the full scrape-and-write process
        sample_writer.run()
        logger.info("Scrape and write process complete.")


class PreciosClarosToDatabase:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting to process samples and update database...")
        ctx = get_precios_claros_context(info_storage)
        
        logger.info(f"Connecting to database: {ctx.db_uri}")
        with get_session(ctx.db_uri) as session:
            repo = DatabaseRepository(session)
            for store in ctx.tree_store.iterate():
                try:
                    logger.info(f"Processing store {store.key} with timestamp {store.timestamp}...")
                    repo.process_sample(store)
                except Exception as e:
                    logger.error(f"Failed to process store {store.key}: {e}", exc_info=True)

        logger.info("Database update process complete.")


class PreciosClarosInspect:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting inspection of scraped data...")
        ctx = get_precios_claros_context(info_storage)
        
        tree_store_stats, point_of_sale_stats, article_stats = get_scrape_stats(ctx.tree_store)
        timestamp = get_timestamp()
        inspection_home = get_directory(ctx.tree_store.home / Sample.REPORTS / "inspection" / timestamp)
        tree_store_stats.report(inspection_home, "tree_store")
        point_of_sale_stats.report(inspection_home, "point_of_sale")
        article_stats.report(inspection_home, "article")
        logger.info(f"Inspection reports saved to: {inspection_home}")


class CleanArticleDescriptions:
    """Action to create canonical tags for article descriptions."""
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting article description tagging...")
        ctx = get_precios_claros_context(info_storage)
        
        logger.info(f"Connecting to database: {ctx.db_uri}")

        with get_session(ctx.db_uri) as session:
            service = ArticleTaggingService(session)
            
            service.generate_high_confidence_tags()
            service.clean_and_match_remaining()
            service.process_rogue_cards()
            
        logger.info("Article description tagging complete.")

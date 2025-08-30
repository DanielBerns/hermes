import logging
from typing import Any

from hermes.domain.sample import Sample
from hermes.domain.sample_writer import SampleWriter
from hermes.domain.data_processor import DataProcessor
from hermes.domain.session import get_session
from hermes.domain.database_repository import DatabaseRepository
from hermes.scrape_precios_claros.sample_builder import SampleBuilder
from hermes.scrape_precios_claros.web_client import WebClient
from hermes.scrape_precios_claros.scraper import Scraper
from hermes.scrape_precios_claros.scrape_stats import get_scrape_stats
from hermes.core.cli import CLI
from hermes.core.constants import DATABASE_NAME_KEY
from hermes.core.formatter import JSONFormatter
from hermes.core.helpers import get_container, get_directory, get_resource, get_timestamp
from hermes.core.rows_selector import RowsSelector
from hermes.core.rows_writer import RowsWriter
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore
from hermes.scrape_precios_claros.scrape_stats import get_scrape_stats

logger = logging.getLogger(__name__)

class ScrapePreciosClarosException(Exception):
    pass

class ScrapePreciosClarosStart:
    """Action to initialize the parameters for a scrape run."""
    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict, config: dict, storage: Storage) -> None:
        logger.info("Initializing Precios Claros scrape parameters...")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        SampleBuilder.initialize_parameters_home(tree_store.home)
        logger.info("Initialization complete.")

class ScrapePreciosClarosUpdate:
    """Action to perform a scrape and save the raw data to files."""
    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict, config: dict, storage: Storage) -> None:
        logger.info("Starting Precios Claros data update scrape...")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        parameters_home = get_container(tree_store.home, Sample.PARAMETERS)

        # Instantiate all the refactored components
        web_client = WebClient()
        scraper = Scraper(web_client)
        data_processor = DataProcessor()
        selector = RowsSelector.read(parameters_home, Sample.STATES_AND_CITIES_SELECTOR)

        sample_builder = SampleBuilder(scraper, data_processor, selector)

        # Create a new store for this run
        store = tree_store.create_store()
        logger.info(f"Created new data store: {store.key}")

        writer = RowsWriter(store.home)
        formatter = JSONFormatter()
        sample_writer = SampleWriter(sample_builder, writer, formatter)

        # Execute the full scrape-and-write process
        sample_writer.run()
        logger.info("Scrape and write process complete.")

class ScrapePreciosClarosToDB:
    """Action to process all saved samples and insert them into the database."""
    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict, config: dict, storage: Storage) -> None:
        logger.info("Starting to process samples and update database...")
        mecon_container = storage.container(Sample.MECON)
        tree_store = TreeStore(mecon_container, Sample.TREE_STORE)

        db_container = storage.container(Sample.DATABASE, base=mecon_container)
        db_name = arguments.get(DATABASE_NAME_KEY, "mecon_dev")
        db_uri = str(get_resource(db_container, db_name, ".db"))
        logger.info(f"Connecting to database: {db_uri}")

        with get_session(db_uri) as session:
            repo = DatabaseRepository(session)
            for store in tree_store.iterate():
                try:
                    logger.info(f"Processing store {store.key} with timestamp {store.timestamp}...")
                    repo.process_sample(store)
                except Exception as e:
                    logger.error(f"Failed to process store {store.key}: {e}", exc_info=True)

        logger.info("Database update process complete.")

class ScrapePreciosClarosInspect:
    """Action to generate statistics and reports from the stored data."""
    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict, config: dict, storage: Storage) -> None:
        logger.info("Starting inspection of scraped data...")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        tree_store_stats, point_of_sale_stats, article_stats = get_scrape_stats(tree_store)

        timestamp = get_timestamp()
        inspection_home = get_directory(tree_store.home / Sample.REPORTS / "inspection" / timestamp)

        tree_store_stats.report(inspection_home, "tree_store")
        point_of_sale_stats.report(inspection_home, "point_of_sale")
        article_stats.report(inspection_home, "article")
        logger.info(f"Inspection reports saved to: {inspection_home}")

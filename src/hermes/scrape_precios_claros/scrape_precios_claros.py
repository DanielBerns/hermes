import logging
from typing import Any
from thefuzz import process, fuzz

from hermes.domain.models import ArticleCard, ArticleTag, ArticleBrand, ArticleDescription, ArticlePackage
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


class TagArticleDescriptions:
    """Action to create canonical tags for article descriptions."""

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict, config: dict, storage: Storage) -> None:
        logger.info("Starting article description tagging...")
        mecon_container = storage.container(Sample.MECON)
        db_container = storage.container(Sample.DATABASE, base=mecon_container)
        db_name = arguments.get(DATABASE_NAME_KEY, "mecon_dev")
        db_uri = str(get_resource(db_container, db_name, ".db"))
        logger.info(f"Connecting to database: {db_uri}")

        with get_session(db_uri) as session:
            # Local cache to track tags within the transaction to prevent IntegrityError
            tag_cache = {}

            def get_or_create_tag(tag_text: str) -> ArticleTag:
                """
                Efficiently gets a tag from cache, DB, or creates a new one.
                This prevents trying to add a duplicate tag within the same transaction.
                """
                # 1. Check local cache first
                if tag_text in tag_cache:
                    return tag_cache[tag_text]

                # 2. If not in cache, check DB
                tag = session.query(ArticleTag).filter_by(tag=tag_text).first()
                if tag:
                    tag_cache[tag_text] = tag  # Add to cache for future lookups
                    return tag

                # 3. If not in DB, create a new one, add to session and cache
                new_tag = ArticleTag(tag=tag_text)
                session.add(new_tag)
                tag_cache[tag_text] = new_tag
                return new_tag

            # --- Phase 1: Generate High-Confidence Tags ---
            logger.info("Phase 1: Generating high-confidence tags...")
            all_cards = session.query(ArticleCard).all()
            for i, card in enumerate(all_cards):
                # Eagerly load related objects to avoid multiple queries per loop
                brand = card.brand.brand
                description = card.description.description

                parts = description.split(brand)
                if len(parts) == 2 and all(parts):
                    tag_text = parts[0].strip()
                    if tag_text:  # Ensure tag is not an empty string
                        tag = get_or_create_tag(tag_text)
                        if tag not in card.tags:
                            card.tags.append(tag)

                if (i + 1) % 500 == 0:
                    logger.info(f"Phase 1: Committing batch, processed {i+1}/{len(all_cards)} cards.")
                    session.commit()

            session.commit()  # Final commit for Phase 1
            logger.info("Phase 1 complete.")

            # --- Phase 2: Clean and Match Remaining Cards ---
            logger.info("Phase 2: Cleaning and matching remaining cards...")

            # Refresh the list of all tag strings from the DB to include newly created tags from Phase 1
            all_tag_strings = [t.tag for t in session.query(ArticleTag).all()]
            untagged_cards = session.query(ArticleCard).filter(~ArticleCard.tags.any()).all()

            for i, card in enumerate(untagged_cards):
                brand = card.brand.brand
                description = card.description.description
                package = card.package.package

                clean_description = description

                # Fuzzy Brand Removal
                result = process.extractOne(brand, clean_description, scorer=fuzz.token_set_ratio)
                if result and result[1] > 80:
                    clean_description = clean_description.replace(result[0], "").strip()

                # Fuzzy Package Removal
                result = process.extractOne(package, clean_description, scorer=fuzz.token_set_ratio)
                if result and result[1] > 80:
                    clean_description = clean_description.replace(result[0], "").strip()

                # Find best matching tag if the cleaned description is not empty
                if clean_description and all_tag_strings:
                    best_match = process.extractOne(clean_description, all_tag_strings)
                    if best_match and best_match[1] > 85:
                        tag = get_or_create_tag(best_match[0])
                        if tag not in card.tags:
                            card.tags.append(tag)

                if (i + 1) % 500 == 0:
                    logger.info(f"Phase 2: Committing batch, processed {i+1}/{len(untagged_cards)} cards.")
                    session.commit()

            session.commit()  # Final commit for Phase 2
            logger.info("Phase 2 complete.")

        logger.info("Article description tagging complete.")

import logging
from typing import Any

from sqlalchemy.orm import joinedload

from hermes.core.config import get_config
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
from hermes.core.cleaning_by_context import search_insertion_context, compare_prefix, levenshtein
from hermes.core.formatter import JSONFormatter
from hermes.core.helpers import get_container, get_directory, get_resource, get_timestamp
from hermes.core.rows_selector import RowsSelector
from hermes.core.rows_writer import RowsWriter
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore

logger = logging.getLogger(__name__)

class PreciosClarosException(Exception):
    pass


class PreciosClarosStart:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Initializing Precios Claros parameters...")
        mecon_container = info_storage.container(Sample.MECON)
        tree_store = TreeStore(
            info_storage.container(Sample.TREE_STORE, base=mecon_container)
        )
        RowsSelector.create(
            tree_store.parameters, Sample.STATES_AND_CITIES_SELECTOR, Sample.STATES_AND_CITIES
        )

        logger.info("Initialization complete.")


class PreciosClarosUpdate:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting Precios Claros data update scrape...")
        mecon_container = info_storage.container(Sample.MECON)
        tree_store = TreeStore(
            info_storage.container(Sample.TREE_STORE, base=mecon_container)
        )

        # Instantiate all the refactored components
        web_client = WebClient()
        scraper = Scraper(web_client)
        data_processor = DataProcessor()
        selector = RowsSelector.read(tree_store.parameters, Sample.STATES_AND_CITIES_SELECTOR)

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


class PreciosClarosToDatabase:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting to process samples and update database...")
        mecon_container = info_storage.container(Sample.MECON)
        tree_store = TreeStore(
            info_storage.container(Sample.TREE_STORE, base=mecon_container)
        )
        config = get_config()
        db_container = info_storage.container(Sample.DATABASE, base=mecon_container)
        db_name = config.database.name
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


class PreciosClarosInspect:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting inspection of scraped data...")
        mecon_container = info_storage.container(Sample.MECON)
        tree_store = TreeStore(
            info_storage.container(Sample.TREE_STORE, base=mecon_container)
        )
        tree_store_stats, point_of_sale_stats, article_stats = get_scrape_stats(tree_store)
        timestamp = get_timestamp()
        inspection_home = get_directory(tree_store.home / Sample.REPORTS / "inspection" / timestamp)
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
        mecon_container = info_storage.container(Sample.MECON)
        tree_store = TreeStore(
            info_storage.container(Sample.TREE_STORE, base=mecon_container)
        )
        config = get_config()
        db_container = info_storage.container(Sample.DATABASE, base=mecon_container)
        db_name = config.database.name
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
            all_tag_strings = sorted([t.tag for t in session.query(ArticleTag).all()])
            untagged_cards = session.query(ArticleCard).filter(~ArticleCard.tags.any()).all()
            for i, card in enumerate(untagged_cards):
                description = card.description.description
                before, after = search_insertion_context(all_tag_strings, description)
                max_len = max(len(before) if before else 0, len(after) if after else 0, len(description) if description else 0)
                if before:
                    logger.info(f"before {before} - {description}")
                    lvs_before = levenshtein(before, description)
                else:
                    lvs_before = len(description)
                if after:
                    logger.info(f"after {after} - {description}")
                    lvs_after = levenshtein(after, description)
                else:
                    lvs_after = len(description)

                min_lvs = min(lvs_before, lvs_after)
                if min_lvs == 0:
                    best_match = description
                else:
                    best_match = before if lvs_before == min_lvs else after
                    if min_lvs * 5 > max_len:
                        before_prefix_len = compare_prefix(before, description) if before else 0
                        after_prefix_len = compare_prefix(after, description) if after else 0
                        best_prefix_len = max(before_prefix_len, after_prefix_len)
                        if best_prefix_len > min_lvs:
                            best_match = description[:best_prefix_len]
                        else:
                            continue
                    else:
                        pass
                if best_match:
                    logger.info(f"best_match {best_match} - {description} - {lvs_before} - {lvs_after}")
                    tag = get_or_create_tag(best_match)
                    if tag not in card.tags:
                        card.tags.append(tag)

                if (i + 1) % 500 == 0:
                    logger.info(f"Phase 2: Committing batch, processed {i+1}/{len(untagged_cards)} cards.")
                    session.commit()

            session.commit()  # Final commit for Phase 2
            logger.info("Phase 2 complete.")

            rogue_cards = (
                    session.query(ArticleCard)
                    .join(ArticleCard.description)  # Join to allow sorting on the description table
                    .options(
                        joinedload(ArticleCard.brand),
                        joinedload(ArticleCard.description),
                        joinedload(ArticleCard.package),
                        joinedload(ArticleCard.code),
                    )
                    .filter(~ArticleCard.tags.any())
                    .order_by(ArticleDescription.description)  # Sort by the description text
                    .all()
            )
            former_card = rogue_cards[0]
            for i, current_card in enumerate(rogue_cards[1:], 1):
                if former_card.brand.id == current_card.brand.id:
                    former_description = former_card.description.description
                    current_description = current_card.description.description
                    prefix_size = compare_prefix(former_description, current_description)
                    if prefix_size > 5:
                        prefix = (current_description[:prefix_size]).strip()
                        logger.info(f"prefix {prefix} - {former_description} - {current_description}")
                        tag = get_or_create_tag(prefix)
                        if tag not in current_card.tags:
                            current_card.tags.append(tag)
                        if tag not in former_card.tags:
                            former_card.tags.append(tag)

                if (i + 1) % 500 == 0:
                    logger.info(f"Phase 3: Committing batch, processed {i+1}/{len(rogue_cards)} cards.")
                    session.commit()
                former_card = current_card

            session.commit()  # Final commit for Phase 2
            logger.info("Phase 3 complete.")

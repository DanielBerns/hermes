import logging
from typing import Any

from hermes.domain.sample import Sample
from hermes.domain.sample_writer import SampleWriter
from hermes.domain.rows_to_db import get_mecon_rows_processors
from hermes.scrape_precios_claros.sample_builder import SampleBuilder
from hermes.scrape_precios_claros.web_client import WebClient
from hermes.scrape_precios_claros.scraper import Scraper
from hermes.scrape_precios_claros.scrape_stats import get_scrape_stats
from hermes.core.cli import CLI
from hermes.core.helpers import get_container, get_timestamp, get_directory
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore
from hermes.core.rows_writer import RowsWriter
from hermes.core.rows_selector import RowsSelector
from hermes.core.formatter import JSONFormatter

logger = logging.getLogger(__name__)


class ScrapePreciosClarosException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class ScrapePreciosClarosStart:
    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(
        self,
        script: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
        storage: Storage,
    ) -> None:
        logger.info(f"{self.__class__.__name__}.run: start")
        logger.info(f"Storage.base: {storage.info_base}")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        SampleBuilder.initialize_parameters_home(tree_store.home)
        logger.info(f"{self.__class__.__name__}.run: done")


class ScrapePreciosClarosUpdate:
    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(
        self,
        script: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
        storage: Storage,
    ) -> None:
        logger.info(f"{self.__class__.__name__}.run: start")
        logger.info(f"Storage.base: {storage.info_base}")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        parameters_home = get_container(tree_store.home, Sample.PARAMETERS)
        if not parameters_home.exists():
            message = f"{self.__class__.__name__}.create_sample: no parameters_home {parameters_home}"
            raise ScrapePreciosClarosException(message)
        web_client = WebClient()
        scraper = Scraper(web_client)
        points_of_sale_processor, articles_by_point_of_sale_processor = (
            get_mecon_rows_processors()
        )
        states_and_cities_selector = RowsSelector.read(
            parameters_home, Sample.STATES_AND_CITIES_SELECTOR
        )
        sample_builder = SampleBuilder(
            scraper,
            points_of_sale_processor,
            states_and_cities_selector,
            articles_by_point_of_sale_processor,
        )
        store = tree_store.create_store()
        writer = RowsWriter(store.home)
        json_formatter = JSONFormatter()
        sample_writer = SampleWriter(sample_builder, writer, json_formatter)
        sample_writer.run()
        logger.info(f"{self.__class__.__name__}.run: done")


class ScrapePreciosClarosInspect:
    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(
        self,
        script: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
        storage: Storage,
    ) -> None:
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        tree_store_stats, point_of_sale_stats, article_stats = get_scrape_stats(
            tree_store
        )
        timestamp = get_timestamp()
        inspection_home = get_directory(
            tree_store.home / Sample.REPORTS / "inspection" / timestamp
        )
        tree_store_stats.report(inspection_home, "tree_store")
        point_of_sale_stats.report(inspection_home, "point_of_sale")
        article_stats.report(inspection_home, "article")

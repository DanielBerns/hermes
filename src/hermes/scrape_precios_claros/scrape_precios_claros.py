import logging
from typing import Any

from hermes.core.cli import CLI
from hermes.domain.sample import Sample
from hermes.domain.sample_reader import SampleReader
from hermes.domain.sample_writer import SampleWriter
from hermes.domain.rows_to_db import (
    PointOfSaleRowsProcessor,
    ArticlesByPointOfSaleRowsProcessor,
    get_city_key,
    get_place_key,
    get_point_of_sale_key,
    get_article_card_key,
    get_mecon_rows_processors,
    price_to_cents,
    cents_to_price
)
from hermes.scrape_precios_claros.sample_builder import SampleBuilder
from hermes.scrape_precios_claros.web_client import WebClient
from hermes.scrape_precios_claros.scraper import Scraper
from hermes.core.helpers import (
    get_resource,
    get_container,
    get_timestamp,
    create_text_file
)
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore
from hermes.core.rows_writer import RowsWriter
from hermes.core.rows_selector import RowsSelector
from hermes.core.formatter import JSONFormatter
from hermes.core.metadata import Metadata

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

    def run(self, script: str, arguments: dict[str, Any], config: dict[str, Any], storage: Storage) -> None:
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

    def run(self, script: str, arguments: dict[str, Any], config: dict[str, Any], storage: Storage) -> None:
        logger.info(f"{self.__class__.__name__}.run: start")
        logger.info(f"Storage.base: {storage.info_base}")
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        parameters_home = get_container(tree_store.home, Sample.PARAMETERS)
        if not parameters_home.exists():
            message = f"{self.__class__.__name__}.create_sample: no parameters_home {parameters_home}"
            raise ScrapePreciosClarosException(message)
        web_client = WebClient()
        scraper = Scraper(web_client)
        points_of_sale_processor, articles_by_point_of_sale_processor = get_mecon_rows_processors()
        states_and_cities_selector = RowsSelector.read(
            parameters_home,
            Sample.STATES_AND_CITIES_SELECTOR
        )
        sample_builder = SampleBuilder(
            scraper,
            points_of_sale_processor,
            states_and_cities_selector,
            articles_by_point_of_sale_processor)
        store = tree_store.create_store()
        writer = RowsWriter(
            store.home
        )
        json_formatter = JSONFormatter()
        sample_writer = SampleWriter(
            sample_builder,
            writer,
            json_formatter
        )
        sample_writer.run()
        logger.info(f"{self.__class__.__name__}.run: done")


class ScrapePreciosClarosInspect:
    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(self, script: str, arguments: dict[str, Any], config: dict[str, Any], storage: Storage) -> None:
        tree_store = TreeStore(storage.container(Sample.MECON), Sample.TREE_STORE)
        reports_home = get_container(tree_store.home, Sample.REPORTS)
        inspection_report_resource = get_resource(reports_home, "inspection", ".md")
        with create_text_file(inspection_report_resource) as text:
            text.write("# Scrape data inspection\n\n")
            for number, this_sample in enumerate(tree_store.iterate()):
                text.write(f"## Sample {number}\n\n")
                text.write(f"1. Index {this_sample.index} with key {this_sample.key} at home {this_sample.home}\n")
                text.write(f"2. Check: {this_sample.index == number}\n")
                reader = SampleReader(this_sample.home)
                states:
                cities:
                for list_index, (key, value) in enumerate(reader.metadata.table.items(), 1):
                    text.write(f"{list_index}. {key}: {value}\n")
                points_of_sale_counter = 0
                for this_point_of_sale in reader.points_of_sale():
                    points_of_sale_counter += 1
                articles_counter = 0
                for this_article in reader.articles_by_point_of_sale():
                    articles_counter += 1
                text.write(f"3. Number of PointOfSale {points_of_sale_counter}\n")
                text.write(f"4. Number of Article {articles_counter}\n")
                reader.metadata.read()
                timestamp = reader.metadata.table.get("timestamp", "00000000000000")
                text.write(f"5. Timestamp {timestamp}\n")
                text.write("\n")
                text.write("### Actual metadata\n\n")
                for cursor, (key, value) in enumerate(reader.metadata.table.items(), 1):
                    text.write(f"{cursor}. {key}: {value}\n")
                text.write("\n")


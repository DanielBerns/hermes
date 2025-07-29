import logging
from pathlib import Path
from typing import Any, List, Generator
from collections import defaultdict

from hermes.domain.sample import Sample
from hermes.scrape.scraper import Scraper, ScraperStop
from hermes.core.rows_processor import RowsProcessor
from hermes.core.rows_selector import RowsSelector
from hermes.core.helpers import (get_container, classname)


# Get a named logger for this module
logger = logging.getLogger(__name__)


class SampleBuilder:

    def __init__(
        self,
        scraper: Scraper,
        points_of_sale_rows_processor: RowsProcessor,
        selector: RowsSelector,
        articles_by_point_of_sale_rows_processor: RowsProcessor
    ) -> None:
        self._scraper: Scraper = scraper
        self._table: List[dict[str, Any]] = []
        self._points_of_sale_rows_processor: RowsProcessor = points_of_sale_rows_processor
        self._selector: RowsSelector = selector
        self._articles_by_point_of_sale_rows_processor: RowsProcessor = articles_by_point_of_sale_rows_processor

    @property
    def scraper(self) -> Scraper:
        return self._scraper

    @property
    def table(self) -> list():
        return self._table

    @property
    def points_of_sale_rows_processor(self) -> RowsProcessor:
        return self._points_of_sale_rows_processor

    @property
    def selector(self) -> RowsSelector:
        return self._selector

    @property
    def articles_by_point_of_sale_rows_processor(self) -> RowsProcessor:
        return self._articles_by_point_of_sale_rows_processor

    def points_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.points_of_sale: start")
        try:
            for this_point_of_sale in self.scraper.points_of_sale():
                row = self.points_of_sale_rows_processor.execute(this_point_of_sale)
                self.table.append(row)
                yield row
        except ScraperStop:
            logger.warning(f"{self.__class__.__name__}.points_of_sale: ScraperStop raised")
        logger.info(f"{self.__class__.__name__}.points_of_sale: done")

    def articles_by_point_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: start")
        for this_point_of_sale in self.table:
            if self.selector.select(this_point_of_sale):
                for this_article in self.scraper.articles_by_point_of_sale(this_point_of_sale[Sample.POINT_OF_SALE_CODE]):
                    processed_article = self.articles_by_point_of_sale_rows_processor.execute(this_article)
                    yield processed_article
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: stop")

    @staticmethod
    def initialize_parameters_home(home: Path) -> None:
        parameters_home = get_container(home, Sample.PARAMETERS)
        RowsSelector.create(
            parameters_home,
            Sample.POINTS_OF_SALE_SELECTOR,
            Sample.STATES_AND_CITIES
        )

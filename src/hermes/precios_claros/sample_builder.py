import logging
from pathlib import Path
from typing import Any, List, Generator

from hermes.domain.sample import Sample
from hermes.scrape_precios_claros.scraper import Scraper, ScraperStop
from hermes.domain.data_processor import DataProcessor
from hermes.core.rows_selector import RowsSelector
from hermes.core.helpers import get_container


logger = logging.getLogger(__name__)


class SampleBuilder:
    """
    Orchestrates the process of scraping, processing, and filtering data
    to build a coherent sample.
    """

    def __init__(
        self,
        scraper: Scraper,
        data_processor: DataProcessor,
        selector: RowsSelector,
    ) -> None:
        self._scraper = scraper
        self._data_processor = data_processor
        self._selector = selector
        # Caches the processed points of sale to avoid re-fetching
        self._processed_points_of_sale: List[dict[str, Any]] = []

    def points_of_sale(self) -> Generator[dict[str, Any], None, None]:
        """
        Fetches raw points of sale from the scraper, processes them,
        caches the results, and yields them.
        """
        logger.info("Starting to build points of sale sample.")
        try:
            for raw_pos in self._scraper.points_of_sale():
                processed_pos = self._data_processor.process_point_of_sale(raw_pos)
                self._processed_points_of_sale.append(processed_pos)
                yield processed_pos
        except ScraperStop:
            logger.warning("Scraping for points of sale was stopped gracefully.")
        logger.info("Finished building points of sale sample.")

    def articles_by_point_of_sale(self) -> Generator[dict[str, Any], None, None]:
        """
        Iterates through cached points of sale, filters them using the selector,
        and then scrapes and processes articles for the selected ones.
        """
        logger.info("Starting to build articles sample.")
        if not self._processed_points_of_sale:
            logger.warning("No points of sale were processed; cannot fetch articles.")
            return

        for pos in self._processed_points_of_sale:
            if self._selector.select(pos):
                pos_code = pos.get(Sample.POINT_OF_SALE_CODE)
                if not pos_code:
                    logger.warning(f"Skipping point of sale with missing code: {pos}")
                    continue

                try:
                    for raw_article in self._scraper.articles_by_point_of_sale(pos_code):
                        processed_article = self._data_processor.process_article(raw_article)
                        yield processed_article
                except ScraperStop:
                    logger.warning(f"Scraping for articles at {pos_code} was stopped gracefully.")

        logger.info("Finished building articles sample.")


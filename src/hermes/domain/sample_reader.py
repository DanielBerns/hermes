import logging
from pathlib import Path
from typing import Any, Generator

from hermes.core.metadata import Metadata
from hermes.core.rows_reader import RowsReader

from hermes.domain.sample import Sample

# Get a named logger for this module
logger = logging.getLogger(__name__)


class SampleReader:
    def __init__(self, base: Path) -> None:
        self._rows_reader: RowsReader = RowsReader(base)
        self._metadata = Metadata(base)

    @property
    def rows_reader(self) -> RowsReader:
        return self._rows_reader

    @property
    def metadata(self) -> Metadata:
        return self._metadata

    def points_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.points_of_sale: start")
        for this_point_of_sale in self.rows_reader.execute(Sample.POINTS_OF_SALE):
            yield this_point_of_sale
        logger.info(f"{self.__class__.__name__}.points_of_sale: done")

    def articles_by_point_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: start")
        try:
            for this_article in self.rows_reader.execute(Sample.ARTICLES):
                yield this_article
        except Exception:
            for this_article in self.rows_reader.execute("articles_per_point_of_sale"):
                yield this_article
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: done")

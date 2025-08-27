import logging
from pathlib import Path
from typing import Any, Generator

from hermes.core.metadata import Metadata
from hermes.core.tree_store import Store
from hermes.core.rows_io import read_rows
from hermes.domain.sample import Sample

# Get a named logger for this module
logger = logging.getLogger(__name__)

class SampleReader:
    def __init__(self, store: Store) -> None:
        self._store = store
        self._metadata = Metadata(store.home)

    @property
    def metadata(self) -> Metadata:
        return self._metadata

    @property
    def store(self) -> Store:
        return self._store

    def timestamp(self) -> dict[str, Any]:
        return {"timestamp": self.store.timestamp}

    def points_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.points_of_sale: start")
        for this_point_of_sale in read_rows(self.store.home, Sample.POINTS_OF_SALE):
            yield this_point_of_sale
        logger.info(f"{self.__class__.__name__}.points_of_sale: done")

    def articles_by_point_of_sale(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: start")
        try:
            for this_article in read_rows(self.store.home, Sample.ARTICLES):
                yield this_article
        except Exception:
            for this_article in read_rows(self.store.home, "articles_per_point_of_sale"):
                yield this_article
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: done")

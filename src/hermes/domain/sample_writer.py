import logging

from hermes.cores.formatter import Formatter
from hermes.cores.rows_writer import RowsWriter

from hermes.domain.sample import Sample
from hermes.domain.sample_generator import SampleGenerator

# Get a named logger for this module
logger = logging.getLogger(__name__)

class SampleWriter:
    def __init__(
        self,
        sample_generator: SampleGenerator,
        rows_writer: RowsWriter,
        formatter: Formatter,
    ) -> None:
        self._sample_generator: SampleGenerator = sample_generator
        self._rows_writer: RowsWriter = rows_writer
        self._formatter: Formatter = formatter

    @property
    def sample_generator(self) -> SampleGenerator:
        return self._sample_generator

    @property
    def rows_writer(self) -> RowsWriter:
        return self._rows_writer

    @property
    def formatter(self) -> Formatter:
        return self._formatter

    def run(self) -> None:
        logger.info(f"{self.__class__.__name__}.run: start")
        self.rows_writer.execute(Sample.POINTS_OF_SALE, self.sample_generator.points_of_sale, self.formatter)
        self.rows_writer.execute(Sample.ARTICLES, self.sample_generator.articles_by_point_of_sale, self.formatter)
        logger.info(f"{self.__class__.__name__}.run: done")


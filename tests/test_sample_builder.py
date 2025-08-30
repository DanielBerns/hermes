import pytest
from unittest.mock import MagicMock, call

from hermes.scrape_precios_claros.sample_builder import SampleBuilder
from hermes.scrape_precios_claros.scraper import Scraper
from hermes.domain.data_processor import DataProcessor
from hermes.core.rows_selector import RowsSelector

@pytest.fixture
def mock_scraper() -> MagicMock:
    return MagicMock(spec=Scraper)

@pytest.fixture
def mock_data_processor() -> MagicMock:
    return MagicMock(spec=DataProcessor)

@pytest.fixture
def mock_selector() -> MagicMock:
    return MagicMock(spec=RowsSelector)

@pytest.fixture
def sample_builder(
    mock_scraper: MagicMock,
    mock_data_processor: MagicMock,
    mock_selector: MagicMock,
) -> SampleBuilder:
    """Provides a SampleBuilder instance with mocked dependencies."""
    return SampleBuilder(mock_scraper, mock_data_processor, mock_selector)


def test_sample_builder_full_workflow(
    sample_builder: SampleBuilder,
    mock_scraper: MagicMock,
    mock_data_processor: MagicMock,
    mock_selector: MagicMock,
):
    """
    Tests the end-to-end workflow of the SampleBuilder, ensuring all
    components are called correctly.
    """
    # --- Arrange: Set up mock return values ---

    # Raw data returned by the scraper
    raw_pos_1 = {"id": "pos-1", "provincia": "Chubut"}
    raw_pos_2 = {"id": "pos-2", "provincia": "Santa Cruz"}
    raw_article_1 = {"id": "art-a", "precio": "100", "point_of_sale_id": "pos-1"}

    # Processed data returned by the data processor
    processed_pos_1 = {"point_of_sale_code": "pos-1", "state": "chubut"}
    processed_pos_2 = {"point_of_sale_code": "pos-2", "state": "santa cruz"}
    processed_article_1 = {"article_code": "art-a", "price": 10000}

    mock_scraper.points_of_sale.return_value = [raw_pos_1, raw_pos_2]
    mock_scraper.articles_by_point_of_sale.return_value = [raw_article_1]

    mock_data_processor.process_point_of_sale.side_effect = [processed_pos_1, processed_pos_2]
    mock_data_processor.process_article.return_value = processed_article_1

    # The selector will only approve the first point of sale
    mock_selector.select.side_effect = lambda pos: pos["point_of_sale_code"] == "pos-1"

    # --- Act: Run the builder's methods ---

    # 1. Generate and process points of sale
    pos_results = list(sample_builder.points_of_sale())

    # 2. Generate and process articles for the selected points of sale
    article_results = list(sample_builder.articles_by_point_of_sale())

    # --- Assert: Verify the results and interactions ---

    # Check the final output
    assert pos_results == [processed_pos_1, processed_pos_2]
    assert article_results == [processed_article_1]

    # Verify that the scraper and processor were called correctly for POS
    mock_scraper.points_of_sale.assert_called_once()
    mock_data_processor.process_point_of_sale.assert_has_calls([
        call(raw_pos_1),
        call(raw_pos_2)
    ])

    # Verify the selector was called for both processed POS
    mock_selector.select.assert_has_calls([
        call(processed_pos_1),
        call(processed_pos_2)
    ])

    # Verify the scraper was only called for the *selected* POS
    mock_scraper.articles_by_point_of_sale.assert_called_once_with("pos-1")

    # Verify the processor was called for the article from the selected POS
    mock_data_processor.process_article.assert_called_once_with(raw_article_1)


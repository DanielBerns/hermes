import pytest
from unittest.mock import MagicMock, call
from hermes.scrape_precios_claros.scraper import Scraper, ScraperError, ScraperStop

@pytest.fixture
def mock_web_client() -> MagicMock:
    """Provides a mock WebClient instance."""
    return MagicMock()

@pytest.fixture
def scraper(mock_web_client: MagicMock) -> Scraper:
    """Provides a Scraper instance initialized with a mock WebClient."""
    return Scraper(mock_web_client)

def test_points_of_sale_happy_path(scraper: Scraper, mock_web_client: MagicMock):
    """Tests the successful retrieval of points of sale over multiple pages."""
    # Mock the initial call to get query limits
    mock_web_client.get.side_effect = [
        # First call for limits
        {"total": 5, "maxLimitPermitido": 2},
        # First page of data
        {"sucursales": [{"id": 1}, {"id": 2}]},
        # Second page of data
        {"sucursales": [{"id": 3}, {"id": 4}]},
        # Last page of data
        {"sucursales": [{"id": 5}]},
    ]

    results = list(scraper.points_of_sale())

    assert len(results) == 5
    assert [item['id'] for item in results] == [1, 2, 3, 4, 5]

    # Verify the calls made to the web client
    base_url = "https://d3e6htiiul5ek9.cloudfront.net/dev/sucursales"
    expected_calls = [
        call(base_url, params={'limit': 0}),
        call(base_url, params={'offset': 0, 'limit': 2}),
        call(base_url, params={'offset': 2, 'limit': 2}),
        call(base_url, params={'offset': 4, 'limit': 1}),
    ]
    mock_web_client.get.assert_has_calls(expected_calls)

def test_articles_by_point_of_sale_with_extra_fields(scraper: Scraper, mock_web_client: MagicMock):
    """Tests that extra fields (like point_of_sale_id) are correctly added to results."""
    pos_id = "test-id-123"
    mock_web_client.get.side_effect = [
        {"total": 2, "maxLimitPermitido": 5},
        {"productos": [{"name": "product A"}, {"name": "product B"}]},
    ]

    results = list(scraper.articles_by_point_of_sale(pos_id))

    assert len(results) == 2
    assert results[0] == {"name": "product A", "point_of_sale_id": pos_id}
    assert results[1] == {"name": "product B", "point_of_sale_id": pos_id}

    # Verify correct parameters were used for the API call
    base_url = "https://d3e6htiiul5ek9.cloudfront.net/dev/productos"
    mock_web_client.get.assert_any_call(base_url, params={'id_sucursal': pos_id, 'limit': 0})
    mock_web_client.get.assert_any_call(base_url, params={'id_sucursal': pos_id, 'offset': 0, 'limit': 2})


def test_no_items_found(scraper: Scraper, mock_web_client: MagicMock):
    """Tests behavior when the API returns zero total items."""
    mock_web_client.get.return_value = {"total": 0, "maxLimitPermitido": 10}

    results = list(scraper.points_of_sale())
    assert len(results) == 0

def test_api_error_on_limits(scraper: Scraper, mock_web_client: MagicMock):
    """Tests that a ScraperError is raised if fetching limits fails."""
    mock_web_client.get.side_effect = Exception("Network Error")

    with pytest.raises(ScraperError, match="Network Error"):
        list(scraper.points_of_sale())

def test_scraper_stop_on_missing_results_key(scraper: Scraper, mock_web_client: MagicMock):
    """Tests that ScraperStop is raised if the data key (e.g., 'sucursales') is missing."""
    mock_web_client.get.side_effect = [
        {"total": 1, "maxLimitPermitido": 1},
        {"unexpected_key": []} # No 'sucursales' key
    ]

    with pytest.raises(ScraperStop):
       list(scraper.points_of_sale())

import pytest
from unittest.mock import MagicMock, patch

from hermes.scrape_precios_claros.scrape_precios_claros import (
    PreciosClarosUpdate,
    PreciosClarosToDB
)
from hermes.core.storage import Storage

@pytest.fixture
def mock_storage() -> MagicMock:
    """Provides a mock Storage object."""
    return MagicMock(spec=Storage)

@pytest.fixture
def mock_config() -> dict:
    """Provides a mock config dictionary."""
    return {"some_key": "some_value"}

@pytest.fixture
def mock_arguments() -> dict:
    """Provides a mock arguments dictionary."""
    return {"database_name": "test.db"}

def test_scrape_precios_claros_update_action(mock_storage, mock_config, mock_arguments, mocker):
    """
    Tests that the PreciosClarosUpdate action correctly instantiates and runs
    the full scraping and writing pipeline.
    """
    # --- Arrange: Mock all dependencies instantiated within the action ---
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.TreeStore")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.WebClient")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.Scraper")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.DataProcessor")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.RowsSelector")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.SampleBuilder")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.RowsWriter")
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.JSONFormatter")
    mock_sample_writer_class = mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.SampleWriter")
    mock_sample_writer_instance = mock_sample_writer_class.return_value

    # --- Act ---
    action = PreciosClarosUpdate()
    action.run("test_script", mock_arguments, mock_config, mock_storage)

    # --- Assert ---
    # Check that the final object in the chain was created and its method was called
    mock_sample_writer_class.assert_called_once()
    mock_sample_writer_instance.run.assert_called_once()

def test_scrape_precios_claros_to_db_action(mock_storage, mock_config, mock_arguments, mocker):
    """
    Tests that the PreciosClarosToDB action correctly iterates through
    stores and calls the DatabaseRepository to process each one.
    """
    # --- Arrange ---
    # Mock the dependencies
    mock_tree_store_class = mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.TreeStore")
    mock_tree_store_instance = mock_tree_store_class.return_value
    mock_repo_class = mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.DatabaseRepository")
    mock_repo_instance = mock_repo_class.return_value
    mocker.patch("hermes.scrape_precios_claros.scrape_precios_claros.get_session")

    # Make the TreeStore iterator return some mock stores
    mock_store1 = MagicMock(key="store1")
    mock_store2 = MagicMock(key="store2")
    mock_tree_store_instance.iterate.return_value = [mock_store1, mock_store2]

    # --- Act ---
    action = PreciosClarosToDB()
    action.run("test_script", mock_arguments, mock_config, mock_storage)

    # --- Assert ---
    # Check that the repository was instantiated
    mock_repo_class.assert_called_once()
    # Check that process_sample was called for each store
    assert mock_repo_instance.process_sample.call_count == 2
    mock_repo_instance.process_sample.assert_any_call(mock_store1)
    mock_repo_instance.process_sample.assert_any_call(mock_store2)


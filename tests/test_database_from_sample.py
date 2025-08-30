import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from hermes.domain.database import Base
from hermes.domain.database_from_sample import DatabaseFromSample, SampleTransformer
from hermes.core.tree_store import Store

# Use an in-memory SQLite database for testing
TEST_DB_URI = "sqlite:///:memory:"


@pytest.fixture(scope="function")
def session() -> Session:
    """Creates a clean, in-memory database session for each test function."""
    engine = create_engine(TEST_DB_URI)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db_session = SessionLocal()
    yield db_session
    db_session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def mock_store(tmp_path: Path) -> Store:
    """Creates a mock store with temporary sample data files for each test function."""
    pos_path = tmp_path / "points_of_sale.jsonl"
    articles_path = tmp_path / "articles.jsonl"

    # Sample data
    sample_pos_data = [
         {"point_of_sale_code": "16-1-1302",
          "state": "ar-a",
          "city": "capital",
          "address": "avda. monse\u00f1or tavella y avda. ex - combatientes de malvina none",
          "flag": "hipermercado libertad",
          "business": "libertad s.a",
          "branch": "hipermercado salta",
          "city_key": "(ar-a)[capital]",
          "place_key": "(ar-a)[(capital)(avda. monse\u00f1or tavella y avda. ex - combatientes de malvina none)]",
          "point_of_sale_key": "(16-1-1302)(hipermercado libertad)"}
    ]
    sample_articles_data = [
         {"article_code": "7791130002431",
          "brand": "finish",
          "description": "abrillantador para lavavajilla finish 400 ml",
          "package": "400.0 ml",
          "price": 1180000,
          "point_of_sale_code": "16-1-1302",
          "point_of_sale_key": "(16-1-1302)(hipermercado libertad)",
          "article_card_key": "[7791130002431](finish)(abrillantador para lavavajilla finish 400 ml)(400.0 ml)"}
    ]

    with open(pos_path, "w") as f:
        for item in sample_pos_data:
            f.write(json.dumps(item) + "\n")

    with open(articles_path, "w") as f:
        for item in sample_articles_data:
            f.write(json.dumps(item) + "\n")

    return Store(
        home=tmp_path,
        index=0,
        key="000000000",
        timestamp="20250829150000"
    )


@pytest.fixture
def db_from_sample(session: Session) -> DatabaseFromSample:
    """Provides a DatabaseFromSample instance initialized with the test session."""
    return DatabaseFromSample(session)


def test_end_to_end_sample_processing(db_from_sample: DatabaseFromSample, mock_store: Store, session: Session, caplog):
    """Integration test for the entire sample processing pipeline using pytest fixtures."""

    # Run the processing method
    db_from_sample.read_and_process_sample(mock_store)

    # Assertions: Verify data was inserted correctly
    from hermes.domain.database import Timestamp, State, City, Place, Flag, Business, Branch, PointOfSale, ArticleCode, ArticleBrand, ArticleDescription, ArticlePackage, ArticleCard, Price

    # Timestamp
    timestamps = session.query(Timestamp).all()
    assert len(timestamps) == 1
    assert timestamps[0].year == 2025

    # Location, POS, Articles
    assert session.query(State).count() == 1
    assert session.query(City).count() == 1
    assert session.query(Place).count() == 1
    assert session.query(Flag).count() == 1
    assert session.query(Business).count() == 1
    assert session.query(Branch).count() == 1
    assert session.query(PointOfSale).count() == 1
    assert session.query(ArticleCode).count() == 1
    assert session.query(ArticleBrand).count() == 1
    assert session.query(ArticleDescription).count() == 1
    assert session.query(ArticlePackage).count() == 1
    assert session.query(ArticleCard).count() == 1

    # Price
    prices = session.query(Price).all()
    assert len(prices) == 1
    assert prices[0].amount == 1180000
    assert prices[0].timestamp_id == timestamps[0].id

    # Test idempotency (running it again should log a warning and return early)
    db_from_sample.read_and_process_sample(mock_store)
    assert "already processed" in caplog.text

    # Verify no new data was added
    assert session.query(Timestamp).count() == 1
    assert session.query(Price).count() == 1


# --- Tests for SampleTransformer ---

@pytest.fixture
def transformer() -> SampleTransformer:
    """Provides a SampleTransformer instance with a mocked DB dependency."""
    mock_db = MagicMock()
    return SampleTransformer(mock_db)


def test_state_from_row(transformer: SampleTransformer):
    row = {"state": "ar-c"}
    expected = {"code": "ar-c", "name": "CABA"}
    assert transformer.state_from_row(row) == expected

    row_invalid = {"state": "invalid-code"}
    expected_invalid = {"code": "xxxx", "name": "Error"}
    assert transformer.state_from_row(row_invalid) == expected_invalid


def test_city_from_row(transformer: SampleTransformer):
    # Mock the return value of the db's get_state method
    mock_state = MagicMock()
    mock_state.id = 1
    transformer._db.get_state.return_value = mock_state

    row = {"state": "ar-b", "city": "la plata"}
    expected = {"name": "la plata", "state_id": 1}

    result = transformer.city_from_row(row)
    assert result == expected

    # Verify that get_state was called with the correct arguments
    transformer._db.get_state.assert_called_with({'code': 'ar-b', 'name': 'Buenos Aires'})










import unittest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from hermes.domain.database import Base, get_session
from hermes.domain.database_from_sample import DatabaseFromSample, SampleTransformer
from hermes.core.tree_store import Store

# Use an in-memory SQLite database for testing
TEST_DB_URI = "sqlite:///:memory:"

class TestDatabaseFromSample(unittest.TestCase):

    def setUp(self):
        """Set up a clean, in-memory database for each test."""
        self.engine = create_engine(TEST_DB_URI)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.db_from_sample = DatabaseFromSample(self.session)

        # Create mock sample data directory and files
        self.mock_store_path = Path("./mock_store")
        self.mock_store_path.mkdir(exist_ok=True)

        self.pos_path = self.mock_store_path / "points_of_sale.jsonl"
        self.articles_path = self.mock_store_path / "articles.jsonl"

        # Sample data
        self.sample_pos_data = [
             {"point_of_sale_code": "16-1-1302",
              "state": "ar-a",
              "city": "capital",
              "address": "avda. monse\u00f1or tavella y avda. ex - combatientes de malvina none",
              "flag": "hipermercado libertad",
              "business": "libertad s.a",
              "branch": "hipermercado salta",
              "city_key": "(ar-a)[capital]",
              "place_key": "(ar-a)[(capital)(avda. monse\u00f1or tavella y avda. ex - combatientes de malvina none)]",
              "point_of_sale_key": "(16-1-1302)(hipermercado libertad)"}
        ]
        self.sample_articles_data = [
             {"article_code": "7791130002431",
              "brand": "finish",
              "description": "abrillantador para lavavajilla finish 400 ml",
              "package": "400.0 ml",
              "price": 1180000,
              "point_of_sale_code": "16-1-1302",
              "point_of_sale_key": "(16-1-1302)(hipermercado libertad)",
              "article_card_key": "[7791130002431](finish)(abrillantador para lavavajilla finish 400 ml)(400.0 ml)"}
        ]

        with open(self.pos_path, "w") as f:
            for item in self.sample_pos_data:
                f.write(json.dumps(item) + "\n")

        with open(self.articles_path, "w") as f:
            for item in self.sample_articles_data:
                f.write(json.dumps(item) + "\n")


    def tearDown(self):
        """Clean up the database and mock files after each test."""
        self.session.close()
        Base.metadata.drop_all(self.engine)

        self.pos_path.unlink()
        self.articles_path.unlink()
        self.mock_store_path.rmdir()

    def test_end_to_end_sample_processing(self):
        """Integration test for the entire sample processing pipeline."""

        # 1. Create a mock store object
        mock_store = Store(
            home=self.mock_store_path,
            index=0,
            key="000000000",
            timestamp="20250829150000"
        )

        # 2. Run the processing method
        self.db_from_sample.read_and_process_sample(mock_store)

        # 3. Assertions: Verify data was inserted correctly
        from hermes.domain.database import Timestamp, State, City, Place, Flag, Business, Branch, PointOfSale, ArticleCode, ArticleBrand, ArticleDescription, ArticlePackage, ArticleCard, Price

        # Timestamp
        timestamps = self.session.query(Timestamp).all()
        self.assertEqual(len(timestamps), 1)
        self.assertEqual(timestamps[0].year, 2025)

        # Location
        self.assertEqual(self.session.query(State).count(), 1)
        self.assertEqual(self.session.query(City).count(), 1)
        self.assertEqual(self.session.query(Place).count(), 1)

        # Point of Sale
        self.assertEqual(self.session.query(Flag).count(), 1)
        self.assertEqual(self.session.query(Business).count(), 1)
        self.assertEqual(self.session.query(Branch).count(), 1)
        self.assertEqual(self.session.query(PointOfSale).count(), 1)

        # Articles
        self.assertEqual(self.session.query(ArticleCode).count(), 1)
        self.assertEqual(self.session.query(ArticleBrand).count(), 1)
        self.assertEqual(self.session.query(ArticleDescription).count(), 1)
        self.assertEqual(self.session.query(ArticlePackage).count(), 1)
        self.assertEqual(self.session.query(ArticleCard).count(), 1)

        # Price
        prices = self.session.query(Price).all()
        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0].amount, 1180000)
        self.assertEqual(prices[0].timestamp_id, timestamps[0].id)

        # Test idempotency (running it again should not add new data or fail)
        # It should raise a warning and return early
        with self.assertLogs(level='WARNING') as cm:
            self.db_from_sample.read_and_process_sample(mock_store)
            self.assertIn("already processed", cm.output[0])

        # Verify no new data was added
        self.assertEqual(self.session.query(Timestamp).count(), 1)
        self.assertEqual(self.session.query(Price).count(), 1)


class TestSampleTransformer(unittest.TestCase):

    def setUp(self):
        # Mock the DatabaseFromSample interface that the transformer uses
        self.mock_db = MagicMock()
        self.transformer = SampleTransformer(self.mock_db)

    def test_state_from_row(self):
        row = {"state": "ar-c"}
        expected = {"code": "ar-c", "name": "CABA"}
        self.assertEqual(self.transformer.state_from_row(row), expected)

        row_invalid = {"state": "invalid-code"}
        expected_invalid = {"code": "xxxx", "name": "Error"}
        self.assertEqual(self.transformer.state_from_row(row_invalid), expected_invalid)

    def test_city_from_row(self):
        # Mock the return value of the db's get_state method
        mock_state = MagicMock()
        mock_state.id = 1
        self.mock_db.get_state.return_value = mock_state

        row = {"state": "ar-b", "city": "la plata"}
        expected = {"name": "la plata", "state_id": 1}

        result = self.transformer.city_from_row(row)
        self.assertEqual(result, expected)
        # Verify that get_state was called with the correct arguments
        self.mock_db.get_state.assert_called_with({'code': 'ar-b', 'name': 'Buenos Aires'})


if __name__ == '__main__':
    unittest.main()

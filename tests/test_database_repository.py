import pytest
import json
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from hermes.domain.database import Base, Timestamp, State, City, Price, PointOfSale, Place
from hermes.domain.database_repository import DatabaseRepository
from hermes.core.tree_store import Store

# --- Test Fixtures ---

@pytest.fixture(scope="function")
def session() -> Generator[Session, None, None]:
    """Creates an in-memory SQLite database for the duration of a test function."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = SessionLocal()
    yield db_session
    db_session.close()

@pytest.fixture
def mock_store(tmp_path: Path) -> Store:
    """Creates a mock Store with sample .jsonl data files."""
    store_home = tmp_path / "store1"
    store_home.mkdir()

    # Sample data that covers multiple entities
    pos_data = [
        {"point_of_sale_key": "(coto-1)(coto)", "state": "ar-u", "city": "comodoro rivadavia", "address": "san martin 500", "flag": "coto", "business": "coto sa", "branch": "coto 1"},
        {"point_of_sale_key": "(carrefour-2)(carrefour)", "state": "ar-c", "city": "belgrano", "address": "cabildo 1000", "flag": "carrefour", "business": "carrefour arg", "branch": "cabildo"}
    ]
    articles_data = [
        {"price": 15000, "article_code": "coke1", "brand": "coca-cola", "description": "gaseosa", "package": "2.25l", "point_of_sale_key": "(coto-1)(coto)"},
        {"price": 25000, "article_code": "pepsi1", "brand": "pepsi", "description": "gaseosa", "package": "2.5l", "point_of_sale_key": "(carrefour-2)(carrefour)"}
    ]

    # Write data to mock files
    with open(store_home / "points_of_sale.jsonl", "w") as f:
        for item in pos_data:
            f.write(json.dumps(item) + "\n")
    with open(store_home / "articles.jsonl", "w") as f:
        for item in articles_data:
            f.write(json.dumps(item) + "\n")

    return Store(home=store_home, index=1, key="000000001", timestamp="20250829191500")


# --- Integration Test ---

def test_process_sample_full_workflow(session: Session, mock_store: Store):
    """
    Tests the entire workflow from reading a sample to inserting all related
    data into the database, ensuring idempotency.
    """
    # --- Arrange ---
    repo = DatabaseRepository(session)

    # --- Act: First Run ---
    repo.process_sample(mock_store)

    # --- Assert: First Run ---
    # Check if data was inserted correctly
    assert session.query(Timestamp).count() == 1
    assert session.query(State).count() == 2 # Chubut and CABA
    assert session.query(City).count() == 2
    assert session.query(Place).count() == 2
    assert session.query(PointOfSale).count() == 2
    assert session.query(Price).count() == 2

    # Check a specific relationship
    coto_pos = session.query(PointOfSale).filter(PointOfSale.code == "(coto-1)(coto)").one()
    assert len(coto_pos.places) == 1
    assert coto_pos.places[0].address == "san martin 500"

    # --- Act: Second Run (for idempotency) ---
    repo_2 = DatabaseRepository(session)
    repo_2.process_sample(mock_store)

    # --- Assert: Second Run ---
    # The counts should NOT have changed
    assert session.query(Timestamp).count() == 1
    assert session.query(State).count() == 2
    assert session.query(Price).count() == 2


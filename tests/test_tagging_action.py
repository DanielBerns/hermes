import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from hermes.domain.models import Base, ArticleCard, ArticleTag, ArticleBrand, ArticleDescription, ArticlePackage
from hermes.scrape_precios_claros.scrape_precios_claros import TagArticleDescriptions
from hermes.core.storage import Storage

@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def test_tagging_action(session):
    # Setup: Populate the in-memory database
    brands = [ArticleBrand(brand="Coca-Cola"), ArticleBrand(brand="Pepsi")]
    descriptions = [
        ArticleDescription(description="Coca-Cola Classic 1.5L"),
        ArticleDescription(description="CocaCola Classic 2L"),
        ArticleDescription(description="Pepsi Regular 500ml"),
        ArticleDescription(description="A very messy description for Pepsi"),
    ]
    packages = [
        ArticlePackage(package="1.5L"),
        ArticlePackage(package="2L"),
        ArticlePackage(package="500ml"),
    ]
    session.add_all(brands + descriptions + packages)
    session.commit()

    cards = [
        ArticleCard(brand_id=1, description_id=1, package_id=1, code_id=1),
        ArticleCard(brand_id=1, description_id=2, package_id=2, code_id=2),
        ArticleCard(brand_id=2, description_id=3, package_id=3, code_id=3),
        ArticleCard(brand_id=2, description_id=4, package_id=3, code_id=4),
    ]
    session.add_all(cards)
    session.commit()

    # Execution
    action = TagArticleDescriptions()
    storage = Storage(config={"INFO_BASE":".", "SECRETS_BASE":"."})

    # Mock the arguments
    arguments = {"database_name": ":memory:"}
    config = {}

    # We need to patch get_session to return our in-memory session
    from unittest.mock import patch
    with patch('hermes.scrape_precios_claros.scrape_precios_claros.get_session') as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = session
        action.run("test_script", arguments, config, storage)

    # Assertions
    # Phase 1
    tag1 = session.query(ArticleTag).filter_by(tag="Classic").one()
    assert tag1 is not None
    card1 = session.query(ArticleCard).filter_by(id=1).one()
    assert tag1 in card1.tags

    # Phase 2
    card2 = session.query(ArticleCard).filter_by(id=2).one()
    assert tag1 in card2.tags
    tag2 = session.query(ArticleTag).filter_by(tag="Regular").one()
    assert tag2 is not None
    card3 = session.query(ArticleCard).filter_by(id=3).one()
    assert tag2 in card3.tags

    # Very messy card should remain untagged
    card4 = session.query(ArticleCard).filter_by(id=4).one()
    assert len(card4.tags) == 0

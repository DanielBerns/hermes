
import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from hermes.domain.models import (
    Base, Timestamp, State, City, Place, PointOfSale, 
    ArticleTag, ArticleBrand, ArticleDescription, ArticlePackage, ArticleCode, ArticleCard,
    Price, Flag, Business, Branch
)
from hermes.reporting.reports import get_price_stats_by_location

@pytest.fixture(scope="function")
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_session = SessionLocal()
    yield db_session
    db_session.close()

def test_get_price_stats_by_location(session: Session):
    # --- Arrange ---
    
    # 1. Location
    state = State(code="TST", name="TestState")
    city = City(name="TestCity", state=state)
    place = Place(address="123 Test St", city=city)
    session.add_all([state, city, place])
    
    # 2. Points of Sale
    flag = Flag(flag="TestFlag")
    business = Business(business="TestBiz")
    branch1 = Branch(branch="Branch1")
    branch2 = Branch(branch="Branch2")
    
    pos1 = PointOfSale(code="POS1", flag=flag, business=business, branch=branch1)
    pos1.places.append(place)
    
    pos2 = PointOfSale(code="POS2", flag=flag, business=business, branch=branch2)
    pos2.places.append(place)
    
    session.add_all([flag, business, branch1, branch2, pos1, pos2])
    
    # 3. Articles & Tags
    tag = ArticleTag(tag="Beverages")
    
    brand1 = ArticleBrand(brand="Coke")
    desc1 = ArticleDescription(description="Can")
    pack1 = ArticlePackage(package="330ml")
    code1 = ArticleCode(code="Coke330")
    card1 = ArticleCard(brand=brand1, description=desc1, package=pack1, code=code1)
    card1.tags.append(tag)
    
    brand2 = ArticleBrand(brand="Pepsi")
    code2 = ArticleCode(code="Pepsi330")
    card2 = ArticleCard(brand=brand2, description=desc1, package=pack1, code=code2)
    card2.tags.append(tag)
    
    session.add_all([tag, brand1, brand2, desc1, pack1, code1, code2, card1, card2])
    
    # 4. Timestamp & Prices
    ts_str = "20250101000000"
    ts = Timestamp.from_string(ts_str)
    
    # Coke Prices: 100 at POS1, 200 at POS2
    price1 = Price(amount=100, timestamp=ts, point_of_sale=pos1, article_code=code1)
    price2 = Price(amount=200, timestamp=ts, point_of_sale=pos2, article_code=code1)
    
    # Pepsi Price: 150 at POS1
    price3 = Price(amount=150, timestamp=ts, point_of_sale=pos1, article_code=code2)
    
    session.add_all([ts, price1, price2, price3])
    session.commit()
    
    # --- Act ---
    report = get_price_stats_by_location(session, ts_str, "TestState", "TestCity")
    
    # --- Assert ---
    # Expected structure:
    # {
    #   "Beverages": {
    #       "Coke Can 330ml": {"min": 100, "max": 200, "avg": 150.0},
    #       "Pepsi Can 330ml": {"min": 150, "max": 150, "avg": 150.0}
    #   }
    # }
    
    assert "Beverages" in report
    beverages = report["Beverages"]
    
    # Full description string construction depends on implementation, usually "Brand Description Package"
    coke_key = "Coke Can 330ml" 
    assert coke_key in beverages
    stats = beverages[coke_key]
    assert stats["min"] == 100
    assert stats["max"] == 200
    assert stats["avg"] == 150.0
    
    pepsi_key = "Pepsi Can 330ml"
    assert pepsi_key in beverages
    stats_p = beverages[pepsi_key]
    assert stats_p["min"] == 150
    assert stats_p["max"] == 150
    assert stats_p["avg"] == 150.0


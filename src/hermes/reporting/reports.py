# hermes/reporting/reports.py

from collections import defaultdict
from typing import Dict, List, Optional
from sqlalchemy.orm import Session, joinedload, selectinload
# ADD ArticleDescription to the imports
from src.hermes.domain.models import (
    ArticleTag, ArticleBrand, ArticleCard, ArticleDescription, ArticlePackage,
    Price, PointOfSale, Place, City, State, Timestamp, ArticleCode
)

def _sort_report_data(report: Dict) -> Dict:
    """Helper function to recursively sort report data."""
    # Sort the main keys
    sorted_report = {k: report[k] for k in sorted(report.keys())}
    for key, value in sorted_report.items():
        if isinstance(value, dict):
            # Sort the nested keys
            sorted_value = {k: value[k] for k in sorted(value.keys())}
            for sub_key, sub_value in sorted_value.items():
                if isinstance(sub_value, list):
                    # Sort the final list of strings
                    sorted_value[sub_key] = sorted(sub_value)
            sorted_report[key] = sorted_value
        elif isinstance(value, list):
            # Sort a top-level list (for brand competition)
            sorted_report[key] = sorted(value)
    return sorted_report


def get_all_tags(session: Session) -> List[str]:
    """Returns a list of all distinct tags."""
    return [t[0] for t in session.query(ArticleTag.tag).distinct().order_by(ArticleTag.tag).all()]


def get_all_brands(session: Session) -> List[str]:
    """Returns a list of all distinct brands."""
    return [b[0] for b in session.query(ArticleBrand.brand).distinct().order_by(ArticleBrand.brand).all()]


def get_report_by_tag(session: Session, tag_filter: str = None) -> Dict[str, Dict[str, List[str]]]:

    """
    Generates a sorted report of brands and articles associated with each tag.
    Refactored to use explicit joins, supporting WriteOnlyMapped relationships
    and improving performance on large datasets.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))

    # Query specific columns instead of loading full objects
    # This works with WriteOnlyMapped relationships because we use join()
    query = (
        session.query(
            ArticleTag.tag,
            ArticleBrand.brand,
            ArticleDescription.description
        )
        .select_from(ArticleTag)
        .join(ArticleTag.article_cards)
        .join(ArticleCard.brand)
        .join(ArticleCard.description)
    )

    if tag_filter:
        query = query.filter(ArticleTag.tag == tag_filter)

    rows = query.all()


    # Iterate over the result tuples (tag_name, brand_name, description_text)
    for tag_name, brand_name, description_text in rows:
        report[tag_name][brand_name].append(description_text)

    return _sort_report_data(report)


def get_report_by_brand(session: Session, brand_filter: str = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a sorted report of tags and articles associated with each brand.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    
    brand_query = session.query(ArticleBrand).order_by(ArticleBrand.brand)
    if brand_filter:
        brand_query = brand_query.filter(ArticleBrand.brand == brand_filter)
        
    brands = brand_query.all()

    for brand in brands:
        cards_for_brand = (
            session.query(ArticleCard)
            .filter(ArticleCard.brand_id == brand.id)
            .options(
                selectinload(ArticleCard.tags),
                joinedload(ArticleCard.description)
            )
            .all()
        )

        for card in cards_for_brand:
            for tag in card.tags:
                if card.description:
                    report[brand.brand][tag.tag].append(card.description.description)

    return _sort_report_data(report)


def get_report_brand_competition(session: Session, target_brand_name: str) -> Dict[str, List[str]]:
    """
    Generates a sorted report of competing brands for a given target brand.
    """
    report: Dict[str, List[str]] = defaultdict(list)
    target_brand = session.query(ArticleBrand).filter_by(brand=target_brand_name).first()

    if not target_brand:
        return {}

    tags_associated_with_target_brand = (
        session.query(ArticleTag)
        .join(ArticleTag.article_cards)
        .filter(ArticleCard.brand_id == target_brand.id)
        .distinct()
        .all()
    )

    for tag in tags_associated_with_target_brand:
        competing_brands = (
            session.query(ArticleBrand)
            .join(ArticleBrand.cards)
            .join(ArticleCard.tags)
            .filter(ArticleTag.id == tag.id)
            .filter(ArticleBrand.id != target_brand.id)
            .distinct()
            .all()
        )
        for brand in competing_brands:
            if brand.brand not in report[tag.tag]:
                report[tag.tag].append(brand.brand)

    return _sort_report_data(report)


def calculate_average_price(prices: List[float]) -> float:
    """Calculates the average price from a list of prices."""
    if not prices:
        return 0.0
    return sum(prices) / len(prices)


def get_price_stats_by_location(
    session: Session, timestamp_str: str, state_name: str, city_name: str, tag_filter: Optional[str] = None
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Generates a report of article prices grouped by tag for a specific location and time.
    Returns:
        {
            "TagName": {
                "ArticleDescription": {"min": str, "max": str, "avg": str},
                ...
            },
            ...
        }
    """
    report: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)

    # 1. Resolve Timestamp
    ts = session.query(Timestamp).filter_by(timestamp=Timestamp.from_string(timestamp_str).timestamp).first()
    if not ts:
        return {}

    # 2. Query Prices with strict filtering
    query = (
        session.query(
            Price.amount,
            ArticleTag.tag,
            ArticleBrand.brand,
            ArticleDescription.description,
            ArticlePackage.package,
            ArticleCode.code
        )

        .join(Price.point_of_sale)
        .join(PointOfSale.places)
        .join(Place.city)
        .join(City.state)
        .join(Price.article_code)
        .join(ArticleCode.cards)
        .join(ArticleCard.tags)
        .join(ArticleCard.brand)
        .join(ArticleCard.description)
        .join(ArticleCard.package)
        .filter(Price.timestamp_id == ts.id)
        .filter(State.name == state_name)
        .filter(City.name == city_name)
    )
    
    if tag_filter:
        query = query.filter(ArticleTag.tag == tag_filter)

    results = query.all()

    # 3. Aggregate in memory
    # structure: temp_storage[tag][article_key] = [price1, price2, ...]
    temp_storage = defaultdict(lambda: defaultdict(list))

    for amount, tag, brand, desc, pack, code in results:
        article_key = f"{code} - {brand} {desc} {pack}"
        temp_storage[tag][article_key].append(amount)


    # 4. Compute stats
    for tag, articles in temp_storage.items():
        for article_key, prices in articles.items():
            report[tag][article_key] = {
                "min": f"{min(prices)/100:10.2f}",
                "max": f"{max(prices)/100:10.2f}",
                "avg": f"{calculate_average_price(prices)/100:10.2f}"
            }

    return _sort_report_data(report)


def get_all_timestamps(session: Session) -> List[str]:
    """Returns a list of all distinct timestamps formatted as strings."""
    # Timestamps are stored as datetime objects in the database
    timestamps = session.query(Timestamp.timestamp).distinct().order_by(Timestamp.timestamp.desc()).all()
    return [ts[0].strftime("%Y%m%d%H%M%S") for ts in timestamps]


def get_all_cities(session: Session) -> List[Dict[str, str]]:
    """Returns a list of all distinct cities with their states."""
    cities = (
        session.query(City.name, State.name)
        .join(City.state)
        .distinct()
        .order_by(State.name, City.name)
        .all()
    )
    return [{"city": city, "state": state} for city, state in cities]



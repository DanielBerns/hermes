# hermes/reporting/reports.py

from collections import defaultdict
from typing import Dict, List, Any
from sqlalchemy.orm import Session, joinedload, selectinload
from sqlalchemy import func
from hermes.domain.database import (
    ArticleTag, ArticleBrand, ArticleCard, ArticleDescription, ArticleCode,
    Price, Timestamp, PointOfSale, Place, City
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
                    # Sort the final list
                    if sub_value and isinstance(sub_value[0], dict):
                        # Sort by description for the new dictionary format
                        sorted_value[sub_key] = sorted(sub_value, key=lambda x: x.get("description", ""))
                    else:
                        # Fallback for simple lists of strings (e.g., brand competition)
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


def get_report_by_tag(session: Session, tag_filter: str = None) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    """
    Generates a sorted report of brands and articles associated with each tag.
    Returns format: { "Tag": { "Brand": [{"description": "...", "code": "..."}] } }
    """
    report: Dict[str, Dict[str, List[Dict[str, str]]]] = defaultdict(lambda: defaultdict(list))

    query = (
        session.query(
            ArticleTag.tag,
            ArticleBrand.brand,
            ArticleDescription.description,
            ArticleCode.code
        )
        .select_from(ArticleTag)
        .join(ArticleTag.article_cards)
        .join(ArticleCard.brand)
        .join(ArticleCard.description)
        .join(ArticleCard.code)
    )

    if tag_filter:
        query = query.filter(ArticleTag.tag == tag_filter)

    rows = query.all()

    for tag_name, brand_name, description_text, code_text in rows:
        report[tag_name][brand_name].append({
            "description": description_text,
            "code": code_text
        })

    return _sort_report_data(report)


def get_report_by_brand(session: Session, brand_filter: str = None) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    """
    Generates a sorted report of tags and articles associated with each brand.
    Returns format: { "Brand": { "Tag": [{"description": "...", "code": "..."}] } }
    """
    report: Dict[str, Dict[str, List[Dict[str, str]]]] = defaultdict(lambda: defaultdict(list))

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
                joinedload(ArticleCard.description),
                joinedload(ArticleCard.code)
            )
            .all()
        )

        for card in cards_for_brand:
            for tag in card.tags:
                if card.description and card.code:
                    report[brand.brand][tag.tag].append({
                        "description": card.description.description,
                        "code": card.code.code
                    })

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


def get_article_price_history(session: Session, article_code: str) -> List[Dict[str, Any]]:
    """
    Retrieves the min and max price for a specific article, grouped by Timestamp and City.
    Ordered by Timestamp and City name.
    """
    query = (
        session.query(
            Timestamp.timestamp,
            City.name.label("city_name"),
            func.min(Price.amount).label("min_price"),
            func.max(Price.amount).label("max_price")
        )
        .select_from(Price)
        .join(Price.timestamp)
        .join(Price.article_code)
        .join(Price.point_of_sale)
        .join(PointOfSale.places)
        .join(Place.city)
        .filter(ArticleCode.code == article_code)
        .group_by(Timestamp.timestamp, City.name)
        .order_by(Timestamp.timestamp, City.name)
    )

    results = []
    for ts, city, min_p, max_p in query.all():
        results.append({
            "timestamp": ts.isoformat(),
            "city": city,
            "min_price": min_p,
            "max_price": max_p
        })
    return results

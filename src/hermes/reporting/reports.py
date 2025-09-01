# hermes/reporting/reports.py

from collections import defaultdict
from typing import Dict, List
from sqlalchemy.orm import Session, joinedload, selectinload
from src.hermes.domain.models import ArticleTag, ArticleBrand, ArticleCard

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

def get_report_by_tag(session: Session) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a sorted report of brands and articles associated with each tag.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    tags = (
        session.query(ArticleTag)
        .options(
            selectinload(ArticleTag.article_cards).joinedload(ArticleCard.brand),
            selectinload(ArticleTag.article_cards).joinedload(ArticleCard.description),
        )
        .all()
    )

    for tag in tags:
        for card in tag.article_cards:
            if card.brand and card.description:
                report[tag.tag][card.brand.brand].append(card.description.description)

    return _sort_report_data(report)


def get_report_by_brand(session: Session) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a sorted report of tags and articles associated with each brand.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    brands = session.query(ArticleBrand).order_by(ArticleBrand.brand).all()

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

# hermes/reporting/reports.py

from collections import defaultdict
from typing import Dict, List
from sqlalchemy.orm import Session, joinedload, selectinload
from hermes.domain.models import ArticleTag, ArticleBrand, ArticleCard

def get_report_by_tag(session: Session) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a report of brands and articles associated with each tag.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    # This function's logic is correct as ArticleTag.article_cards is a standard Mapped[List] relationship
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
    return dict(report)


def get_report_by_brand(session: Session) -> Dict[str, Dict[str, List[str]]]:
    """
    Generates a report of tags and articles associated with each brand.
    This version correctly queries related cards from a WriteOnlyMapped collection.
    """
    report: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))

    # 1. Fetch all brands
    brands = session.query(ArticleBrand).all()

    # 2. Iterate through each brand
    for brand in brands:
        # 3. Create a NEW query for ArticleCard, filtering by the current brand.
        #    Apply the necessary eager loading options to THIS query.
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

    return dict(report)


def get_report_brand_competition(session: Session, target_brand_name: str) -> Dict[str, List[str]]:
    """
    Generates a report of competing brands for a given target brand.
    """
    report: Dict[str, List[str]] = defaultdict(list)
    target_brand = session.query(ArticleBrand).filter_by(brand=target_brand_name).first()

    if not target_brand:
        return {}

    # Find all tags associated with the target brand's cards
    tags_associated_with_target_brand = (
        session.query(ArticleTag)
        .join(ArticleTag.article_cards)
        .filter(ArticleCard.brand_id == target_brand.id)
        .distinct()
        .all()
    )

    # For each tag, find all other brands associated with it
    for tag in tags_associated_with_target_brand:
        competing_brands = (
            session.query(ArticleBrand)
            .join(ArticleBrand.cards) # This join works because it's translated to SQL
            .join(ArticleCard.tags)
            .filter(ArticleTag.id == tag.id)
            .filter(ArticleBrand.id != target_brand.id)
            .distinct()
            .all()
        )
        for brand in competing_brands:
            if brand.brand not in report[tag.tag]:
                report[tag.tag].append(brand.brand)

    return dict(report)

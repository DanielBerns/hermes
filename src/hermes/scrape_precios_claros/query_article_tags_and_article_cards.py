import logging
import sqlalchemy as sa
from sqlalchemy.orm import joinedload

from hermes.core.helpers import get_resource
from hermes.core.config import get_config
from hermes.core.storage import Storage

from hermes.domain.models import ArticleCard, ArticleTag, ArticleBrand, ArticleDescription, ArticlePackage
from hermes.domain.sample import Sample

from hermes.domain.session import get_session

logger = logging.getLogger(__name__)

def get_untagged_article_cards(session):
    """
    Queries the database for all ArticleCards that have no associated ArticleTags.

    Args:
        session: The SQLAlchemy session object.

    Returns:
        A list of ArticleCard objects that are not associated with any tag.
    """
    return (
        session.query(ArticleCard)
        .options(
            joinedload(ArticleCard.brand),
            joinedload(ArticleCard.description),
            joinedload(ArticleCard.package),
            joinedload(ArticleCard.code),
        )
        .filter(~ArticleCard.tags.any())
        .all()
    )

def get_sorted_tags_with_cards(session):
    """
    Queries the database for all ArticleTags, sorted alphabetically by tag,
    and eagerly loads their associated ArticleCards with all related details.

    Args:
        session: The SQLAlchemy session object.

    Returns:
        A list of ArticleTag objects with their article_cards pre-loaded.
    """
    return (
        session.query(ArticleTag)
        .options(
            joinedload(ArticleTag.article_cards)
            .joinedload(ArticleCard.brand),
            joinedload(ArticleTag.article_cards)
            .joinedload(ArticleCard.description),
            joinedload(ArticleTag.article_cards)
            .joinedload(ArticleCard.package),
            joinedload(ArticleTag.article_cards)
            .joinedload(ArticleCard.code),
        )
        .order_by(ArticleTag.tag)
        .all()
    )

def generate_tagged_cards_report(tags_with_cards):
    """
    Generates a markdown report from a list of ArticleTags and their cards.

    Args:
        tags_with_cards: A list of ArticleTag objects from get_sorted_tags_with_cards.

    Returns:
        A string containing the report in Markdown format.
    """
    report_lines = ["# ArticleCard by Article Tag"]

    if not tags_with_cards:
        report_lines.append("\nNo article tags found in the database.")
        return "\n".join(report_lines)

    for tag in tags_with_cards:
        report_lines.append(f"\n## {tag.tag}")
        if not tag.article_cards:
            report_lines.append("\n1. No associated articles for this tag.")
        else:
            for i, card in enumerate(tag.article_cards, 1):
                # The description of an ArticleCard is its brand, description, and package combined.
                description = (
                    f"{card.brand.brand} {card.description.description} "
                    f"({card.package.package})"
                )
                report_lines.append(f"{i}. {description}")

    return "\n".join(report_lines)

def generate_untagged_cards_report(untagged_cards):
    """
    Generates a markdown report for untagged ArticleCards.

    Args:
        untagged_cards: A list of untagged ArticleCard objects.

    Returns:
        A string containing the report in Markdown format.
    """
    report_lines = ["# Article Cards Without Tags"]

    if not untagged_cards:
        report_lines.append("\nAll article cards have at least one tag.")
        return "\n".join(report_lines)

    for i, card in enumerate(untagged_cards, 1):
        description = (
            f"{card.brand.brand} >> {card.description.description}"
        )
        report_lines.append(f"{i}. {description}")

    return "\n".join(report_lines)


class QueryArticleTagsAndArticleCards:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Starting article description tagging...")
        mecon_container = info_storage.container(Sample.MECON)
        report_container = info_storage.container("query_article_tags_and_article_cards")
        config = get_config()
        db_container = info_storage.container(Sample.DATABASE, base=mecon_container)
        db_name = config.database.name
        db_uri = str(get_resource(db_container, db_name, ".db"))
        logger.info(f"Connecting to database: {db_uri}")

        with get_session(db_uri) as session:

             # 1. Query the database to get the sorted data
             tagged_article_cards = get_sorted_tags_with_cards(session)
             untagged_article_cards = get_untagged_article_cards(session)

             # 2. Generate the markdown report
             tagged_cards_output = generate_tagged_cards_report(tagged_article_cards)
             untagged_cards_output = generate_untagged_cards_report(untagged_article_cards)

             # 3. Print and save the report
             logger.info("--- Generated Report ---")
             try:
                 resource = get_resource(report_container, "tagged_cards", ".md")
                 with open(resource, "w") as f:
                     f.write(tagged_cards_output)
                 logger.info("\nReport successfully saved to {resource}")
             except IOError as e:
                 logger.error(f"\nError saving report file: {e}")

             try:
                 resource = get_resource(report_container, "untagged_cards", ".md")
                 with open(resource, "w") as f:
                     f.write(untagged_cards_output)
                 logger.info("\nReport successfully saved to {resource}")
             except IOError as e:
                 logger.error(f"\nError saving report file: {e}")

             session.commit()  # Final commit

        logger.info("done.")

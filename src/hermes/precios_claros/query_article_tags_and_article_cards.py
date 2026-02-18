import logging

from hermes.core.helpers import get_resource
from hermes.core.storage import Storage
from hermes.domain.database_repository import DatabaseRepository
from hermes.domain.models import ArticleCard, ArticleTag
from hermes.domain.session import get_session
from hermes.scrape_precios_claros.context import get_precios_claros_context

logger = logging.getLogger(__name__)

def generate_tagged_cards_report(tags_with_cards):
    """
    Generates a markdown report from a list of ArticleTags and their cards.

    Args:
        tags_with_cards: A list of ArticleTag objects.

    Returns:
        A string containing the report in Markdown format.
    """
    report_lines = ["# ArticleCard by Article Tag"]

    if not tags_with_cards:
        report_lines.append("\nNo article tags found in the database.")
        return "\n".join(report_lines)

    for tag, cards in tags_with_cards:
        report_lines.append(f"\n## {tag.tag}")
        if not cards:
            report_lines.append("\n1. No associated articles for this tag.")
        else:
            for i, card in enumerate(cards, 1):
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
        
        ctx = get_precios_claros_context(info_storage)
        report_container = ctx.mecon_container / "query_article_tags_and_article_cards"
        
        logger.info(f"Connecting to database: {ctx.db_uri}")

        with get_session(ctx.db_uri) as session:
             repo = DatabaseRepository(session)

             # 1. Query the database to get the sorted data
             tagged_article_cards = repo.get_sorted_tags_with_cards()
             untagged_article_cards = repo.get_untagged_article_cards()

             # 2. Generate the markdown report
             tagged_cards_output = generate_tagged_cards_report(tagged_article_cards)
             untagged_cards_output = generate_untagged_cards_report(untagged_article_cards)

             # 3. Print and save the report
             logger.info("--- Generated Report ---")
             try:
                 resource = get_resource(report_container, "tagged_cards", ".md")
                 with open(resource, "w") as f:
                     f.write(tagged_cards_output)
                 logger.info(f"\nReport successfully saved to {resource}")
             except IOError as e:
                 logger.error(f"\nError saving report file: {e}")

             try:
                 resource = get_resource(report_container, "untagged_cards", ".md")
                 with open(resource, "w") as f:
                     f.write(untagged_cards_output)
                 logger.info(f"\nReport successfully saved to {resource}")
             except IOError as e:
                 logger.error(f"\nError saving report file: {e}")

             session.commit()  # Final commit

        logger.info("done.")

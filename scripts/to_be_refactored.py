from pathlib import Path

from hermes.core.action import execute
from hermes.scrape_precios_claros.query_article_tags_and_article_cards import QueryArticleTagsAndArticleCards


def main() -> None:
    filename = Path(__file__)
    script, project_identifier = filename.stem, filename.parents[1].stem
    action = QueryArticleTagsAndArticleCards()
    execute(script, project_identifier, action)

if __name__ == "__main__":
    main()

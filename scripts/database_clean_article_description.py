from pathlib import Path

from hermes.core.action import execute
from hermes.precios_claros.database_clean_article_description import DatabaseCleanArticleDescription

def main() -> None:
    filename = Path(__file__)
    script, project_identifier = filename.stem, filename.parents[1].stem
    action = DatabaseCleanArticleDescription()
    execute(script, project_identifier, action)

if __name__ == "__main__":
    main()

from pathlib import Path

from hermes.core.action import execute
from hermes.scrape_precios_claros.scrape_precios_claros import ScrapePreciosClarosToDB


def main() -> None:
    filename = Path(__file__)
    script, project_identifier = filename.stem, filename.parents[1].stem
    action = ScrapePreciosClarosToDB()
    execute(script, project_identifier, action)


if __name__ == "__main__":
    main()

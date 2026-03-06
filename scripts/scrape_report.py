import argparse
from pathlib import Path
from hermes.scrape.carrefour.report import generate_markdown_report
from hermes.core.helpers import get_timestamp

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a Markdown price report from the SQLite database.")
    parser.add_argument("--db", default="supermarket_prices.db", help="database filename (default: supermarket_prices.db)")

    args = parser.parse_args()

    webdeprecios_home = Path.home() /  "Info" / "webdeprecios"
    db_file_path = Path(webdeprecios_home, args.db)
    db_uri = f"sqlite:///{db_file_path}"

    timestamp = get_timestamp()
    target = webdeprecios_home / f"latest_prices_{timestamp}.md"

    print(db_uri)

    print(f"Connecting to database: {args.db}")
    generate_markdown_report(db_uri, target)

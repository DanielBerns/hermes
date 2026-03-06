import argparse
from pathlib import Path

from hermes.scrape.carrefour.load import parse_scraping_file, update_database

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse scraped product data and load it into an SQLite database.")
    parser.add_argument(
        "timestamp",
        type=str,
        help="timestamp of the data to be processed"
    )

    parser.add_argument("--db", default="supermarket_prices.db", help="database filename (default: supermarket_prices.db)")

    args = parser.parse_args()

    webdeprecios_home = Path.home() /  "Info" / "webdeprecios"
    db_file_path = Path(webdeprecios_home, args.db)
    db_uri = f"sqlite:///{db_file_path}"

    print(db_uri)

    file_path = webdeprecios_home / f"{timestamp}_results.txt"
    if not file_path.exists():
        print(f"Error: File '{file_path}' not found.")
    else:
        print(f"Processing {file_path}...")
        parsed_data = parse_scraping_file(file_path)
        update_database(db_uri, parsed_data)

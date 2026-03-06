import argparse
from hermes.scrape.report_timeserie import generate_timeseries_report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a Markdown time-series report from the SQLite database.")
    parser.add_argument("--db", default="supermarket_prices.db", help="database filename (default: supermarket_prices.db)")

    args = parser.parse_args()

    webdeprecios_home = Path.home() /  "Info" / "webdeprecios"
    db_file_path = Path(webdeprecios_home, args.db)
    db_uri = f"sqlite:///{db_file_path}"

    timestamp = get_timestamp()
    target = webdeprecios_home / f"price_timeseries_{timestamp}.md"

    generate_timeseries_report(args.db, target)

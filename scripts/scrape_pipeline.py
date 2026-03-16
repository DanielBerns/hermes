import argparse
import asyncio
import logging
import shutil
import sys
from pathlib import Path

from message_board_client.core import MessageBoardClient

# --- Hermes Imports ---
from hermes.core.helpers import get_timestamp
from hermes.scrape.carrefour.extract import CarrefourExtract
from hermes.scrape.carrefour.transform import CarrefourTransform
from hermes.scrape.carrefour.models import Offer
from hermes.scrape.carrefour.load import parse_scraping_file, update_database
from hermes.scrape.carrefour.report_timeserie import generate_timeseries_report

def setup_logging() -> logging.Logger:
    """Configures console logging with clear formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

def send_pipeline_message(config_path: str, message: str, tags: list[str]) -> None:
    """Helper to send an async public message synchronously."""
    async def _send():
        async with MessageBoardClient(config_path) as client:
            await client.send_public_message(tags, message)
    try:
        asyncio.run(_send())
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to send public message via MessageBoardClient: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Carrefour ETL Pipeline")
    parser.add_argument("--config", dest="config_path", required=True, help="Path to MessageBoardClient config.yaml")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Starting Carrefour ETL Pipeline...")
    send_pipeline_message(args.config_path, "Starting Carrefour ETL Pipeline...", ["etl", "carrefour", "info", "start"])

    # --- 0. Configuration & Path Setup ---
    webdeprecios_home = Path.home() / "Info" / "webdeprecios"
    webdeprecios_home.mkdir(parents=True, exist_ok=True)

    # Searches definition
    searches_txt = webdeprecios_home / "searches.txt"
    if not searches_txt.exists():
        default_searches_txt = Path(__file__).parents[1] / "src" / "hermes" / "assets" / "searches.txt"
        if default_searches_txt.exists():
            shutil.copy(default_searches_txt, searches_txt)
            logger.info(f"Copied default searches.txt to {searches_txt}")
        else:
            logger.error(f"Cannot find default searches.txt at {default_searches_txt}")
            send_pipeline_message(args.config_path, f"Cannot find default searches.txt at {default_searches_txt}", ["etl", "carrefour", "error"])
            sys.exit(1)

    # Database configuration
    db_name = "supermarket_prices.db"
    db_file_path = webdeprecios_home / db_name
    db_uri = f"sqlite:///{db_file_path}"

    # Session identifiers
    timestamp = get_timestamp()
    target_dir = webdeprecios_home / timestamp
    results_txt = webdeprecios_home / f"{timestamp}_results.txt"
    report_target = webdeprecios_home / f"price_timeseries_{timestamp}.md"

    # --- 1. EXTRACT ---
    logger.info(f"--- [1/4] Starting Extraction: Session {timestamp} ---")
    try:
        extractor = CarrefourExtract(webdeprecios_home, timestamp, str(searches_txt))
        path_to_driver = str(Path.home() / 'Software' / 'geckodriver')
        path_to_browser = str(Path('/', 'usr', 'bin', 'firefox'))
        extractor.execute(path_to_driver, path_to_browser, headless=True)
        logger.info("Extraction complete.")
    except Exception as e:
        logger.critical(f"Extraction failed: {e}", exc_info=True)
        send_pipeline_message(args.config_path, f"Extraction failed: {e}", ["etl", "carrefour", "error"])
        sys.exit(1)

    # --- 2. TRANSFORM ---
    logger.info("--- [2/4] Starting Transformation ---")
    if not target_dir.exists():
        logger.error(f"Transformation failed: extraction target directory '{target_dir}' does not exist.")
        send_pipeline_message(args.config_path, f"Transformation failed: extraction target directory '{target_dir}' does not exist.", ["etl", "carrefour", "error"])
        sys.exit(1)

    try:
        transformer = CarrefourTransform(target_dir=target_dir)
        results = transformer.execute()
        logger.info(f"Transformation complete. Yielded {len(results)} valid product records.")

        # Write results to text file matching the expected format for 'load.py'
        with open(results_txt, "w") as f:
            f.write(f"Carrefour - {timestamp}\n\n")
            for res in results:
                f.write(f"\nProduct: {res.product.name}\n")
                if res.product.offers:
                    if isinstance(res.product.offers, Offer):
                        an_offer = res.product.offers
                        f.write(f"    {an_offer.sku} Price: {an_offer.priceCurrency} {an_offer.price} - {an_offer.availability} - {an_offer.itemCondition}")
                        if an_offer.seller:
                            f.write(f" - {an_offer.seller.name}")
                        f.write("\n")
                    else:
                        aggregate_offer = res.product.offers
                        offers = aggregate_offer.offers
                        f.write(f"  AggregateOffer {aggregate_offer.priceCurrency} ({aggregate_offer.lowPrice}, {aggregate_offer.highPrice})\n")
                        for an_offer in offers:
                            f.write(f"    {an_offer.sku} Price: {an_offer.priceCurrency} {an_offer.price} - {an_offer.availability} - {an_offer.itemCondition}")
                            if an_offer.seller:
                                f.write(f" - {an_offer.seller.name}")
                            f.write("\n")
        logger.info(f"Saved transformed data to '{results_txt}'.")
    except Exception as e:
        logger.critical(f"Transformation failed: {e}", exc_info=True)
        send_pipeline_message(args.config_path, f"Transformation failed: {e}", ["etl", "carrefour", "error"])
        sys.exit(1)

    # --- 3. LOAD ---
    logger.info("--- [3/4] Starting Load ---")
    if not results_txt.exists():
        logger.error(f"Load failed: transformed results file '{results_txt}' not found.")
        send_pipeline_message(args.config_path, f"Load failed: transformed results file '{results_txt}' not found.", ["etl", "carrefour", "error"])
        sys.exit(1)

    try:
        logger.info(f"Loading '{results_txt}' into database {db_name}...")
        parsed_data_generator = parse_scraping_file(results_txt)
        update_database(db_uri, parsed_data_generator)
        logger.info("Database load complete.")
    except Exception as e:
        logger.critical(f"Database Load failed: {e}", exc_info=True)
        send_pipeline_message(args.config_path, f"Database Load failed: {e}", ["etl", "carrefour", "error"])
        sys.exit(1)

    # --- 4. REPORT ---
    logger.info("--- [4/4] Starting Report Generation ---")
    try:
        generate_timeseries_report(db_uri, report_target)
        logger.info(f"Time-series report generated successfully at '{report_target}'.")
    except Exception as e:
        logger.critical(f"Report generation failed: {e}", exc_info=True)
        send_pipeline_message(args.config_path, f"Report generation failed: {e}", ["etl", "carrefour", "error"])
        sys.exit(1)

    logger.info("--- ETL Pipeline Completed Successfully ---")
    send_pipeline_message(args.config_path, "ETL Pipeline Completed Successfully", ["etl", "carrefour", "success", "end"])

if __name__ == "__main__":
    main()

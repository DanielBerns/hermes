import logging
import shutil
import sys
from pathlib import Path

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

def main() -> None:
    logger = setup_logging()
    logger.info("Starting Carrefour ETL Pipeline...")

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
        sys.exit(1)

    # --- 2. TRANSFORM ---
    logger.info("--- [2/4] Starting Transformation ---")
    if not target_dir.exists():
        logger.error(f"Transformation failed: extraction target directory '{target_dir}' does not exist.")
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
        sys.exit(1)

    # --- 3. LOAD ---
    logger.info("--- [3/4] Starting Load ---")
    if not results_txt.exists():
        logger.error(f"Load failed: transformed results file '{results_txt}' not found.")
        sys.exit(1)

    try:
        logger.info(f"Loading '{results_txt}' into database {db_name}...")
        parsed_data_generator = parse_scraping_file(results_txt)
        update_database(db_uri, parsed_data_generator)
        logger.info("Database load complete.")
    except Exception as e:
        logger.critical(f"Database Load failed: {e}", exc_info=True)
        sys.exit(1)

    # --- 4. REPORT ---
    logger.info("--- [4/4] Starting Report Generation ---")
    try:
        generate_timeseries_report(db_uri, report_target)
        logger.info(f"Time-series report generated successfully at '{report_target}'.")
    except Exception as e:
        logger.critical(f"Report generation failed: {e}", exc_info=True)
        sys.exit(1)

    logger.info("--- ETL Pipeline Completed Successfully ---")

if __name__ == "__main__":
    main()

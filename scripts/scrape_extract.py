import logging
import sys
from pathlib import Path
from hermes.scrape.components import CarrefourExtractor
from hermes.scrape.components import Offer

def setup_logging():
    """Configures console logging with clear formatting."""
    logging.basicConfig(
        level=logging.DEBUG, # Change to logging.INFO for a cleaner output
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    # Specify your target directory here
    target_dir = Path("~", "Info", "webdeprecios", "20260221181203").expanduser()

    # Create the directory for testing purposes if it doesn't exist
    target_dir.mkdir(exist_ok=True)

    try:
        extractor = CarrefourExtractor(target_folder=target_dir)
        results = extractor.process_folder()

        logger.info(f"Extraction complete. Yielded {len(results)} valid product records.")

        # Example of interacting with the clean data
        with open("./results.txt", "w") as results_txt:
            for res in results:
                results_txt.write(f"\nProduct: {res.product.name}\n")
                if res.product.offers:
                    offers = [res.product.offers] if isinstance(res.product.offers, Offer) else res.product.offers.offers
                    for an_offer in offers:
                        results_txt.write(f"  Price: {res.product.offers.priceCurrency} {an_offer.price}")
    except Exception as e:
        logger.critical(f"Application encountered a fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()

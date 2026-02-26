import argparse
import logging
import sys
from pathlib import Path
from hermes.scrape.carrefour.transform import CarrefourTransform
from hermes.scrape.carrefour.models import Offer

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

    parser = argparse.ArgumentParser(description="Transform supermarket prices in html pages in 'ready to load in database' data.")

    parser.add_argument(
        "timestamp",
        type=str,
        help="timestamp of the data to be processed"
    )

    args = parser.parse_args()

    webdeprecios_home = Path.home() /  "Info" / "webdeprecios"
    target_dir = webdeprecios_home / args.timestamp
    if target_dir.exists() == False:
        logger.error("unexpected error: {target_dir} doesn't exists")
        sys.exit(1)
    results_txt = webdeprecios_home / f"{args.timestamp}_results.txt"

    try:
        transformer = CarrefourTransform(target_dir=target_dir)
        results = transformer.execute()

        logger.info(f"Extraction complete. Yielded {len(results)} valid product records.")

        with open(results_txt, "w") as results_txt:
            results_txt.write(f"Carrefour - {args.timestamp}\n\n")
            for res in results:
                results_txt.write(f"\nProduct: {res.product.name}\n")
                if res.product.offers:
                    if isinstance(res.product.offers, Offer):
                        an_offer = res.product.offers
                        results_txt.write(f"    {an_offer.sku} Price: {an_offer.priceCurrency} {an_offer.price} - {an_offer.availability} - {an_offer.itemCondition}")
                        if an_offer.seller:
                            results_txt.write(f" - {an_offer.seller.name}")
                        results_txt.write("\n")
                    else:
                        aggregate_offer = res.product.offers
                        offers = aggregate_offer.offers
                        results_txt.write(f"  AggregateOffer {aggregate_offer.priceCurrency} ({aggregate_offer.lowPrice}, {aggregate_offer.highPrice})\n")
                        for an_offer in offers:
                            results_txt.write(f"    {an_offer.sku} Price: {an_offer.priceCurrency} {an_offer.price} - {an_offer.availability} - {an_offer.itemCondition}")
                            if an_offer.seller:
                                results_txt.write(f" - {an_offer.seller.name}")
                            results_txt.write("\n")
    except Exception as e:
        logger.critical(f"Application encountered a fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    main()

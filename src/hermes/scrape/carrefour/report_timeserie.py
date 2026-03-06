from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from hermes.scrape.carrefour.scrape_extract import Product, PriceReport, Base # Ensure your previous script is named parser.py

def generate_timeseries_report(db_url, output_filepath):
    """Queries the database and writes a Markdown time-series report per product."""
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch products that have at least one price report, ordered alphabetically
        products = session.query(Product).filter(Product.prices.any()).order_by(Product.name).all()

        if not products:
            print("No data found in the database. Run the scraping parser first.")
            return

        with open(output_filepath, 'w', encoding='utf-8') as md_file:
            # YAML Frontmatter
            md_file.write("---\n")
            md_file.write("title: Historical Price Report\n")
            md_file.write("layout: report\n")
            md_file.write("description: Time-series data for tracked supermarket articles.\n")
            md_file.write("---\n\n")

            md_file.write("# Historical Price Report\n\n")

            records_processed = 0

            for product in products:
                # Fetch all price reports for this product, ordered chronologically
                history = session.query(PriceReport)\
                    .filter_by(product_id=product.id)\
                    .order_by(PriceReport.scraped_at.asc())\
                    .all()

                # Write Product Header
                md_file.write(f"## {product.name}\n\n")

                # Write Markdown Table Header
                md_file.write("| Date | Price (ARS) | Store | SKU |\n")
                md_file.write("|---|---|---|---|\n")

                for record in history:
                    # Format data safely
                    price_str = f"${record.price:,.2f}" if record.price is not None else "Out of Stock"
                    store_str = record.store if record.store else "N/A"
                    sku_str = record.sku if record.sku else "N/A"
                    date_str = record.scraped_at.strftime("%Y-%m-%d %H:%M")

                    # Write row
                    md_file.write(f"| {date_str} | {price_str} | {store_str} | {sku_str} |\n")
                    records_processed += 1

                md_file.write("\n---\n\n") # Separator between products

        print(f"Successfully generated time-series report: '{output_filepath}'")
        print(f"Processed {len(products)} products and {records_processed} historical data points.")

    except Exception as e:
        print(f"An error occurred while generating the report: {e}")
    finally:
        session.close()


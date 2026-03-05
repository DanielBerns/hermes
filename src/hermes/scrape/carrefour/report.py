from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from hermes.scrape.carrefour.load import Product, PriceReport, Base # Imports models from the previous script

def generate_markdown_report(db_url, output_filepath):
    """Queries the database for the latest prices and writes a Markdown report."""
    engine = create_engine(db_url)

    # Ensure tables exist (in case this is run before the DB is fully populated)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Fetch all products
        products = session.query(Product).order_by(Product.name).all()

        if not products:
            print("No products found in the database. Run the scraper parser first.")
            return

        with open(output_filepath, 'w', encoding='utf-8') as md_file:
            md_file.write("# Supermarket Price Report\n\n")
            md_file.write("Latest scraped prices for all tracked items.\n\n")

            # Markdown Table Header
            md_file.write("| Product | Price (ARS) | Store | Last Updated |\n")
            md_file.write("|---|---|---|---|\n")

            records_written = 0

            for product in products:
                # Get the most recent price report for this product
                latest_report = session.query(PriceReport)\
                    .filter_by(product_id=product.id)\
                    .order_by(PriceReport.scraped_at.desc())\
                    .first()

                if latest_report and latest_report.price is not None:
                    # Format data safely
                    price_str = f"${latest_report.price:,.2f}"
                    store_str = latest_report.store if latest_report.store else "N/A"
                    date_str = latest_report.scraped_at.strftime("%Y-%m-%d %H:%M")

                    # Write row
                    md_file.write(f"| {product.name} | {price_str} | {store_str} | {date_str} |\n")
                    records_written += 1

        print(f"Successfully generated markdown report: '{output_filepath}' with {records_written} items.")

    except Exception as e:
        print(f"An error occurred while generating the report: {e}")
    finally:
        session.close()



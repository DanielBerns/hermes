from typing import Any
from pathlib import Path
import re
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

# --- Database Models ---

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    # Relationship to historical price records
    prices = relationship("PriceReport", back_populates="product", cascade="all, delete-orphan")

class PriceReport(Base):
    __tablename__ = 'price_reports'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    sku = Column(String, nullable=True)
    price = Column(Float, nullable=True)
    store = Column(String, nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow)

    product = relationship("Product", back_populates="prices")

# --- Parsing Logic ---

def extract_timestamp_from_filename(file_path: Path) -> datetime:
    """Extracts the datetime object from a filename like YYYYMMDDhhmmss_results.txt"""
    filename = file_path.name
    match = re.search(r'(\d{14})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d%H%M%S')
    return datetime.now()

def parse_scraping_file(file_path: Path) -> dict[str, Any]:
    """Reads the text file and yields dictionaries of parsed product data."""
    timestamp = extract_timestamp_from_filename(file_path)
    current_product = None

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # Clean out the dynamically injected tags
            line = re.sub(r'\\s*', '', line).strip()

            if line.startswith('Product:'):
                current_product = line.replace('Product:', '').strip()

            elif 'Price: ARS' in line and current_product:
                # Expected format: SKU Price: ARS 999 - url - url - STORE
                # Or: None Price: ARS None - None - None
                parts = line.split('-')
                price_segment = parts[0].strip()

                # Match the SKU and Price using regex
                match = re.match(r'(\w+)\s+Price:\s+ARS\s+([\d\.]+|None)', price_segment)

                if match:
                    sku_str = match.group(1)
                    price_str = match.group(2)

                    sku = None if sku_str == 'None' else sku_str
                    price = None if price_str == 'None' else float(price_str)

                    # Extract store from the last segment if available
                    store = parts[-1].strip() if len(parts) > 1 else None
                    store = None if store == 'None' else store

                    yield {
                        'name': current_product,
                        'sku': sku,
                        'price': price,
                        'store': store,
                        'scraped_at': timestamp
                    }

# --- Database Operations ---

def update_database(db_url, data_generator):
    """Upserts products and inserts price reports."""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    records_added = 0

    try:
        for item in data_generator:
            # Get or Create the Product
            product = session.query(Product).filter_by(name=item['name']).first()
            if not product:
                product = Product(name=item['name'])
                session.add(product)
                session.flush() # Flush to get the product.id

            # Insert the Price Report
            price_report = PriceReport(
                product_id=product.id,
                sku=item['sku'],
                price=item['price'],
                store=item['store'],
                scraped_at=item['scraped_at']
            )
            session.add(price_report)
            records_added += 1

        session.commit()
        print(f"Successfully added {records_added} price records to the database.")

    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()


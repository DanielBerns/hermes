from sqlalchemy.orm import Session
from .models import ScrapedResult
from .db_models import Brand, Product, Offer

def ingest_scraped_result(session: Session, result: ScrapedResult) -> Product:
    """
    Takes a validated Pydantic ScrapedResult and loads it into the database.
    """
    p_data = result.product

    # 1. Handle the Brand (Upsert logic)
    db_brand = None
    brand_name = None

    # Brand can be a Pydantic model, a string, or None based on the extractor
    if hasattr(p_data.brand, 'name') and p_data.brand.name:
        brand_name = p_data.brand.name
    elif isinstance(p_data.brand, str):
        brand_name = p_data.brand

    if brand_name:
        db_brand = session.query(Brand).filter_by(name=brand_name).first()
        if not db_brand:
            db_brand = Brand(name=brand_name)
            session.add(db_brand)
            session.flush() # Get the ID without committing

    # 2. Handle the Product (Upsert logic using SKU or source_id)
    # Prefer SKU for matching, fallback to the @id (source_id)
    db_product = None
    if p_data.sku:
        db_product = session.query(Product).filter_by(sku=p_data.sku).first()
    if not db_product and p_data.item_id:
        db_product = session.query(Product).filter_by(source_id=p_data.item_id).first()

    if not db_product:
        db_product = Product(
            source_id=p_data.item_id,
            name=p_data.name,
            brand_id=db_brand.id if db_brand else None,
            image_url=str(p_data.image) if p_data.image else None,
            description=p_data.description,
            mpn=p_data.mpn,
            sku=p_data.sku,
            gtin=p_data.gtin
        )
        session.add(db_product)
        session.flush()

    # 3. Handle the Offer (Always insert a new record for time-series tracking)
    if p_data.offers:
        new_offer = Offer(
            product_id=db_product.id,
            price=p_data.offers.price,
            price_currency=p_data.offers.priceCurrency,
            low_price=p_data.offers.lowPrice,
            high_price=p_data.offers.highPrice,
            availability=p_data.offers.availability,
            item_condition=p_data.offers.itemCondition,
            price_valid_until=p_data.offers.priceValidUntil,
            offer_count=p_data.offers.offerCount
        )
        session.add(new_offer)

    return db_product

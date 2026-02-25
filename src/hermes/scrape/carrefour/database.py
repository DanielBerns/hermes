from datetime import datetime
from typing import Optional, List
from sqlalchemy import String, Float, Integer, ForeignKey, DateTime, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Brand(Base):
    __tablename__ = "brands"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)

    # Relationship back to products
    products: Mapped[List["Product"]] = relationship(back_populates="brand")

class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[Optional[str]] = mapped_column(String(500), unique=True, index=True, doc="The @id from JSON-LD")
    name: Mapped[str] = mapped_column(String(500))
    brand_id: Mapped[Optional[int]] = mapped_column(ForeignKey("brands.id"))

    # Text types for potentially long fields
    image_url: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Identifiers
    mpn: Mapped[Optional[str]] = mapped_column(String(100))
    sku: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    gtin: Mapped[Optional[str]] = mapped_column(String(100), index=True)

    # Relationships
    brand: Mapped[Optional["Brand"]] = relationship(back_populates="products")
    offers: Mapped[List["Offer"]] = relationship(back_populates="product", cascade="all, delete-orphan")

class Offer(Base):
    __tablename__ = "offers"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))

    # Pricing details
    price: Mapped[Optional[float]] = mapped_column(Float)
    price_currency: Mapped[Optional[str]] = mapped_column(String(10))
    low_price: Mapped[Optional[float]] = mapped_column(Float)
    high_price: Mapped[Optional[float]] = mapped_column(Float)

    # Metadata
    availability: Mapped[Optional[str]] = mapped_column(String(100))
    item_condition: Mapped[Optional[str]] = mapped_column(String(100))
    price_valid_until: Mapped[Optional[str]] = mapped_column(String(100))
    offer_count: Mapped[Optional[int]] = mapped_column(Integer)

    # Critical for time-series aggregation and reporting
    scraped_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    # Relationship
    product: Mapped["Product"] = relationship(back_populates="offers")

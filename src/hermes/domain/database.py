import logging
from datetime import datetime
from typing import Any, List

import sqlalchemy as sa
from sqlalchemy import Index
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    WriteOnlyMapped,
    mapped_column,
    relationship,
)

from hermes.domain.rows_ops import get_int

# Set up a logger for this module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DatabaseException(Exception):
    """Custom exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""

    pass


class Timestamp(Base):
    """Represents a specific point in time, broken down into its components."""

    __tablename__ = "timestamps"

    # Define a composite index for efficient date-range queries.
    __table_args__ = (Index("ix_timestamp_ymdh", "year", "month", "day", "hour"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    year: Mapped[int] = mapped_column(nullable=False)
    month: Mapped[int] = mapped_column(nullable=False)
    day: Mapped[int] = mapped_column(nullable=False)
    hour: Mapped[int] = mapped_column(nullable=False)
    minute: Mapped[int] = mapped_column(nullable=False)
    second: Mapped[int] = mapped_column(nullable=False)

    # One-to-many relationship with Price.
    prices: WriteOnlyMapped[List["Price"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="timestamp"
    )

    def __repr__(self) -> str:
        """Returns a string representation of the timestamp."""
        return f"{self.year:4d}{self.month:02d}{self.day:02d}{self.hour:02d}{self.minute:02d}{self.second:02d}"

    def to_datetime(self) -> datetime:
        """Converts the Timestamp object to a standard datetime object."""
        return datetime(
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )

    @classmethod
    def from_datetime(cls, dt: datetime) -> "Timestamp":
        """Creates a Timestamp object from a standard datetime object."""
        return cls(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
        )

    def date_string(self) -> str:
        """Returns a formatted date string (YYYY/MM/DD)."""
        return f"{self.year:4d}/{self.month:02d}/{self.day:02d}"

    @classmethod
    def from_string(cls, string: str) -> "Timestamp":
        """Creates a Timestamp object from a formatted string (YYYYMMDDHHMMSS)."""
        _year = get_int(string[0:4])
        _month = get_int(string[4:6])
        _day = get_int(string[6:8])
        _hour = get_int(string[8:10])
        _minute = get_int(string[10:12])
        _second = get_int(string[12:14])
        return cls(
            year=_year,
            month=_month,
            day=_day,
            hour=_hour,
            minute=_minute,
            second=_second,
        )

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Timestamp":
        timestamp_string = row.get("timestamp", "00000000000000")
        return Timestamp.from_string(timestamp_string)


class State(Base):
    __tablename__ = "states"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(sa.String(8))
    name: Mapped[str] = mapped_column(sa.String(32), index=True)

    cities: WriteOnlyMapped[List["City"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="state"
    )


class City(Base):
    __tablename__ = "cities"
    id: Mapped[int] = mapped_column(primary_key=True)
    # Indexing the 'name' column for faster searches.
    name: Mapped[str] = mapped_column(sa.String(32), index=True)
    state_id: Mapped[int] = mapped_column(sa.ForeignKey("states.id"))
    state: Mapped["State"] = relationship(back_populates="cities")
    places: WriteOnlyMapped[List["Place"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="city"
    )


class Flag(Base):
    """Represents a brand or flag of a point of sale"""

    __tablename__ = "flags"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Indexing the 'name' column for faster searches.
    name: Mapped[str] = mapped_column(sa.String(32), index=True)

    points_of_sale: WriteOnlyMapped[List["PointOfSale"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="flag"
    )


class Business(Base):
    """Represents the business entity that owns a point of sale."""

    __tablename__ = "businesses"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Indexing the 'name' column for faster searches.
    name: Mapped[str] = mapped_column(sa.String(32), index=True)

    points_of_sale: WriteOnlyMapped[List["PointOfSale"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="business"
    )


class Branch(Base):
    """Represents a specific branch of a business."""

    __tablename__ = "branches"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(32))
    point_of_sale: Mapped["PointOfSale"] = relationship(
        back_populates="branch", uselist=False
    )


# Association table for the many-to-many relationship between PointOfSale and Place.
points_of_sale_and_places = sa.Table(
    "points_of_sale_and_places",
    Base.metadata,
    sa.Column("point_of_sale_id", sa.ForeignKey("points_of_sale.id"), primary_key=True),
    sa.Column("place_id", sa.ForeignKey("places.id"), primary_key=True),
)


class Place(Base):
    __tablename__ = "places"
    id: Mapped[int] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(sa.String(32))
    city_id: Mapped[int] = mapped_column(sa.ForeignKey("cities.id"), index=True)
    city: Mapped["City"] = relationship(back_populates="places")

    points_of_sale: Mapped[List["PointOfSale"]] = relationship(
        secondary=points_of_sale_and_places, back_populates="places"
    )


class PointOfSale(Base):
    """The central model representing a single point of sale"""

    __tablename__ = "points_of_sale"

    id: Mapped[int] = mapped_column(primary_key=True)

    code: Mapped[str] = mapped_column(unique=True, index=True)

    # Indexing foreign keys to speed up JOIN operations.
    flag_id: Mapped[int] = mapped_column(
        sa.ForeignKey("flags.id"), nullable=False, index=True
    )
    flag: Mapped["Flag"] = relationship(back_populates="points_of_sale")

    business_id: Mapped[int] = mapped_column(
        sa.ForeignKey("businesses.id"), nullable=False, index=True
    )
    business: Mapped["Business"] = relationship(back_populates="points_of_sale")

    branch_id: Mapped[int] = mapped_column(
        sa.ForeignKey("branches.id"), nullable=False, index=True
    )
    branch: Mapped["Branch"] = relationship(back_populates="point_of_sale")

    places: Mapped[List["Place"]] = relationship(
        secondary=points_of_sale_and_places, back_populates="points_of_sale"
    )

    prices: WriteOnlyMapped[List["Price"]] = relationship(
        cascade="all, delete-orphan",
        passive_deletes=True,
        back_populates="point_of_sale",
    )


class ArticleCode(Base):
    """Represents the unique code for an article or product."""

    __tablename__ = "article_codes"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The unique constraint automatically creates an index.
    code: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: Mapped[List["ArticleCard"]] = relationship(back_populates="code")
    prices: WriteOnlyMapped[List["Price"]] = relationship(back_populates="article_code")


class ArticleBrand(Base):
    """Represents the brand of an article."""

    __tablename__ = "article_brands"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The unique constraint automatically creates an index.
    brand: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="brand"
    )


class ArticleDescription(Base):
    """Represents the description of an article."""

    __tablename__ = "article_descriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The unique constraint automatically creates an index.
    description: Mapped[str] = mapped_column(
        sa.String(128), unique=True, nullable=False
    )

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="description"
    )


class ArticlePackage(Base):
    """Represents the packaging information for an article (e.g., '1L bottle')."""

    __tablename__ = "article_packages"

    id: Mapped[int] = mapped_column(primary_key=True)
    # The unique constraint automatically creates an index.
    package: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="package"
    )


class ArticleCard(Base):
    """Combines various attributes to define a unique article."""

    __tablename__ = "article_cards"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Indexing foreign keys to speed up JOIN operations.
    brand_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_brands.id"), index=True
    )
    brand: Mapped["ArticleBrand"] = relationship(back_populates="cards")

    description_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_descriptions.id"), index=True
    )
    description: Mapped["ArticleDescription"] = relationship(back_populates="cards")

    package_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_packages.id"), index=True
    )
    package: Mapped["ArticlePackage"] = relationship(back_populates="cards")

    code_id: Mapped[int] = mapped_column(sa.ForeignKey("article_codes.id"), index=True)
    code: Mapped["ArticleCode"] = relationship(back_populates="cards")


class Price(Base):
    """Represents the price of an article at a specific point of sale at a given time."""

    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Indexing amount for faster price-range queries.
    amount: Mapped[int] = mapped_column(nullable=False, index=True)

    # Indexing foreign keys to speed up JOIN operations.
    timestamp_id: Mapped[int] = mapped_column(
        sa.ForeignKey("timestamps.id"), index=True
    )
    timestamp: Mapped["Timestamp"] = relationship(back_populates="prices")

    article_code_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_codes.id"), index=True
    )
    article_code: Mapped["ArticleCode"] = relationship(back_populates="prices")

    point_of_sale_id: Mapped[int] = mapped_column(
        sa.ForeignKey("points_of_sale.id"), index=True
    )
    point_of_sale: Mapped["PointOfSale"] = relationship(back_populates="prices")

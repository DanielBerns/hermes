# hermes-main/src/hermes/domain/models.py
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy import (
    Index,
    String,
    ForeignKey,
    Table,
    Column,
    Integer,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    WriteOnlyMapped,
    mapped_column,
    relationship,
)
from typing import Any, List

from hermes.domain.data_processor import get_int


# --- Base Class ---
class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""

    pass


# --- Association Tables ---
points_of_sale_and_places = Table(
    "points_of_sale_and_places",
    Base.metadata,
    Column("point_of_sale_id", ForeignKey("points_of_sale.id"), primary_key=True),
    Column("place_id", ForeignKey("places.id"), primary_key=True),
)

article_cards_and_tag_dictionary = Table(
    "article_cards_and_tag_dictionary",
    Base.metadata,
    Column("article_card_id", ForeignKey("article_cards.id"), primary_key=True),
    Column("tag_dictionary_id", ForeignKey("tag_dictionary.id"), primary_key=True),
)


# --- Model Definitions ---
class TagDictionary(Base):
    __tablename__ = "tag_dictionary"
    id: Mapped[int] = mapped_column(primary_key=True)
    tag_name: Mapped[str] = mapped_column(String(256), unique=True, nullable=False)
    parent_id: Mapped[int] = mapped_column(ForeignKey("tag_dictionary.id"), nullable=True)

    parent = relationship("TagDictionary", remote_side=[id], back_populates="children")
    children = relationship("TagDictionary", back_populates="parent")


class Timestamp(Base):
    """Represents a specific point in time, broken down into its components."""

    __tablename__ = "timestamps"
    __table_args__ = (Index("ix_timestamp_ymdh", "year", "month", "day", "hour"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    year: Mapped[int] = mapped_column(nullable=False)
    month: Mapped[int] = mapped_column(nullable=False)
    day: Mapped[int] = mapped_column(nullable=False)
    hour: Mapped[int] = mapped_column(nullable=False)
    minute: Mapped[int] = mapped_column(nullable=False)
    second: Mapped[int] = mapped_column(nullable=False)

    prices: WriteOnlyMapped[List["Price"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="timestamp"
    )

    def __repr__(self) -> str:
        return f"{self.year:4d}{self.month:02d}{self.day:02d}{self.hour:02d}{self.minute:02d}{self.second:02d}"

    def to_datetime(self) -> datetime:
        return datetime(
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )

    @classmethod
    def from_datetime(cls, dt: datetime) -> "Timestamp":
        return cls(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
        )

    @classmethod
    def from_string(cls, string: str) -> "Timestamp":
        return cls(
            year=get_int(string[0:4]),
            month=get_int(string[4:6]),
            day=get_int(string[6:8]),
            hour=get_int(string[8:10]),
            minute=get_int(string[10:12]),
            second=get_int(string[12:14]),
        )


class State(Base):
    __tablename__ = "states"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(sa.String(8), unique=True)
    name: Mapped[str] = mapped_column(sa.String(32), index=True)

    cities: WriteOnlyMapped[List["City"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="state"
    )


class City(Base):
    __tablename__ = "cities"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(32), index=True)
    state_id: Mapped[int] = mapped_column(sa.ForeignKey("states.id"))

    state: Mapped["State"] = relationship(back_populates="cities")
    places: WriteOnlyMapped[List["Place"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="city"
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


class Flag(Base):
    __tablename__ = "flags"
    id: Mapped[int] = mapped_column(primary_key=True)
    flag: Mapped[str] = mapped_column(sa.String(32), unique=True, index=True)

    points_of_sale: WriteOnlyMapped[List["PointOfSale"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="flag"
    )


class Business(Base):
    __tablename__ = "businesses"
    id: Mapped[int] = mapped_column(primary_key=True)
    business: Mapped[str] = mapped_column(sa.String(32), unique=True, index=True)

    points_of_sale: WriteOnlyMapped[List["PointOfSale"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="business"
    )


class Branch(Base):
    __tablename__ = "branches"
    id: Mapped[int] = mapped_column(primary_key=True)
    branch: Mapped[str] = mapped_column(sa.String(32), unique=True)

    point_of_sale: Mapped["PointOfSale"] = relationship(
        back_populates="branch", uselist=False
    )


class PointOfSale(Base):
    __tablename__ = "points_of_sale"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(unique=True, index=True)
    flag_id: Mapped[int] = mapped_column(
        sa.ForeignKey("flags.id"), nullable=False, index=True
    )
    business_id: Mapped[int] = mapped_column(
        sa.ForeignKey("businesses.id"), nullable=False, index=True
    )
    branch_id: Mapped[int] = mapped_column(
        sa.ForeignKey("branches.id"), nullable=False, index=True
    )

    flag: Mapped["Flag"] = relationship(back_populates="points_of_sale")
    business: Mapped["Business"] = relationship(back_populates="points_of_sale")
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
    __tablename__ = "article_codes"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: Mapped[List["ArticleCard"]] = relationship(back_populates="code")
    prices: WriteOnlyMapped[List["Price"]] = relationship(back_populates="article_code")


class ArticleBrand(Base):
    __tablename__ = "article_brands"
    id: Mapped[int] = mapped_column(primary_key=True)
    brand: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="brand"
    )


class ArticleDescription(Base):
    __tablename__ = "article_descriptions"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str] = mapped_column(
        sa.String(128), unique=True, nullable=False
    )

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="description"
    )


class ArticlePackage(Base):
    __tablename__ = "article_packages"
    id: Mapped[int] = mapped_column(primary_key=True)
    package: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)

    cards: WriteOnlyMapped[List["ArticleCard"]] = relationship(
        cascade="all, delete-orphan", passive_deletes=True, back_populates="package"
    )


class ArticleCard(Base):
    __tablename__ = "article_cards"
    id: Mapped[int] = mapped_column(primary_key=True)
    brand_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_brands.id"), index=True
    )
    description_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_descriptions.id"), index=True
    )
    package_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_packages.id"), index=True
    )
    code_id: Mapped[int] = mapped_column(sa.ForeignKey("article_codes.id"), index=True)

    brand: Mapped["ArticleBrand"] = relationship(back_populates="cards")
    description: Mapped["ArticleDescription"] = relationship(back_populates="cards")
    package: Mapped["ArticlePackage"] = relationship(back_populates="cards")
    code: Mapped["ArticleCode"] = relationship(back_populates="cards")
    tags: Mapped[List["TagDictionary"]] = relationship(
        secondary=article_cards_and_tag_dictionary, backref="article_cards"
    )


class Price(Base):
    __tablename__ = "prices"
    id: Mapped[int] = mapped_column(primary_key=True)
    amount: Mapped[int] = mapped_column(nullable=False, index=True)
    timestamp_id: Mapped[int] = mapped_column(
        sa.ForeignKey("timestamps.id"), index=True
    )
    article_code_id: Mapped[int] = mapped_column(
        sa.ForeignKey("article_codes.id"), index=True
    )
    point_of_sale_id: Mapped[int] = mapped_column(
        sa.ForeignKey("points_of_sale.id"), index=True
    )

    timestamp: Mapped["Timestamp"] = relationship(back_populates="prices")
    article_code: Mapped["ArticleCode"] = relationship(back_populates="prices")
    point_of_sale: Mapped["PointOfSale"] = relationship(back_populates="prices")

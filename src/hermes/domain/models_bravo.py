import sqlalchemy as sa
from datetime import datetime
from sqlalchemy import (
    Integer, String, ForeignKey, Table, DateTime,
    Boolean, Index
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    WriteOnlyMapped,
    mapped_column,
    relationship,
)
from typing import List
import bcrypt


# --- Base Class ---

class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""
    pass

# --- Association Tables ---
points_of_sale_and_places = sa.Table(
    "points_of_sale_and_places",
    Base.metadata,
    sa.Column("point_of_sale_id", sa.ForeignKey("points_of_sale.id"), primary_key=True),
    sa.Column("place_id", sa.ForeignKey("places.id"), primary_key=True),
)

article_cards_and_article_tags = sa.Table(
    "article_cards_and_article_tags",
    Base.metadata,
    sa.Column("article_card_id", sa.ForeignKey("article_cards.id"), primary_key=True),
    sa.Column("article_tag_id", sa.ForeignKey("article_tags.id"), primary_key=True),
)

shopping_list_articles = sa.Table(
    'shopping_list_articles',
    Base.metadata,
    sa.Column('shopping_list_id', sa.ForeignKey('shopping_lists.id'), primary_key=True),
    sa.Column('article_code_id', sa.ForeignKey('article_codes.id'), primary_key=True)
)


# --- Model Definitions ---

class ArticleTag(Base):
    __tablename__ = "article_tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    tag: Mapped[str] = mapped_column(String(256), unique=True, nullable=False, index=True)

    article_cards: WriteOnlyMapped["ArticleCard"] = relationship(
        secondary=article_cards_and_article_tags, back_populates="tags"
    )

    def __repr__(self):
        return f"<ArticleTag({self.id}) '{self.tag}'>"

class Timestamp(Base):
    __tablename__ = "timestamps"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    prices: WriteOnlyMapped["Price"] = relationship(back_populates="timestamp")

    def __repr__(self) -> str:
        return self.timestamp.strftime("%Y%m%d%H%M%S")

    @classmethod
    def from_string(cls, string: str) -> "Timestamp":
        year = int(string[0:4])
        month = int(string[4:6])
        day = int(string[6:8])
        hour = int(string[8:10])
        minute = int(string[10:12])
        second = int(string[12:14])
        return cls(timestamp=datetime(year, month, day, hour, minute, second))

class State(Base):
    __tablename__ = "states"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(sa.String(8), unique=True, index=True)
    name: Mapped[str] = mapped_column(sa.String(32), index=True)

    cities: WriteOnlyMapped["City"] = relationship(back_populates="state")

    def __repr__(self):
        return f"<State {self.name}>"

class City(Base):
    __tablename__ = "cities"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(32), index=True)
    state_id: Mapped[int] = mapped_column(sa.ForeignKey("states.id"))

    state: Mapped["State"] = relationship(back_populates="cities")
    places: WriteOnlyMapped["Place"] = relationship(back_populates="city")

    def __repr__(self):
        return f"<City {self.name} at {self.state.name}>"

class Place(Base):
    __tablename__ = "places"
    id: Mapped[int] = mapped_column(primary_key=True)
    address: Mapped[str] = mapped_column(sa.String(32))
    city_id: Mapped[int] = mapped_column(sa.ForeignKey("cities.id"), index=True)

    city: Mapped["City"] = relationship(back_populates="places")
    points_of_sale: Mapped[List["PointOfSale"]] = relationship(
        secondary=points_of_sale_and_places, back_populates="places"
    )

    def __repr__(self):
        return f"<Place {self.address}, {self.city.name}, {self.city.state.name}>"

class Flag(Base):
    __tablename__ = "flags"
    id: Mapped[int] = mapped_column(primary_key=True)
    flag: Mapped[str] = mapped_column(sa.String(32), unique=True, index=True)
    points_of_sale: WriteOnlyMapped["PointOfSale"] = relationship(back_populates="flag")

    def __repr__(self):
        return f"<Flag({self.id}) {self.flag}>"

class Business(Base):
    __tablename__ = "businesses"
    id: Mapped[int] = mapped_column(primary_key=True)
    business: Mapped[str] = mapped_column(sa.String(32), unique=True, index=True)
    points_of_sale: WriteOnlyMapped["PointOfSale"] = relationship(back_populates="business")

class Branch(Base):
    __tablename__ = "branches"
    id: Mapped[int] = mapped_column(primary_key=True)
    branch: Mapped[str] = mapped_column(sa.String(32), unique=True, index=True)
    point_of_sale: Mapped["PointOfSale"] = relationship(back_populates="branch")

class PointOfSale(Base):
    __tablename__ = "points_of_sale"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(unique=True, index=True)
    flag_id: Mapped[int] = mapped_column(sa.ForeignKey("flags.id"), nullable=False, index=True)
    business_id: Mapped[int] = mapped_column(sa.ForeignKey("businesses.id"), nullable=False, index=True)
    # The unique constraint on branch_id makes this a one-to-one relationship
    branch_id: Mapped[int] = mapped_column(sa.ForeignKey("branches.id"), unique=True, nullable=False, index=True)

    flag: Mapped["Flag"] = relationship(back_populates="points_of_sale")
    business: Mapped["Business"] = relationship(back_populates="points_of_sale")
    branch: Mapped["Branch"] = relationship(back_populates="point_of_sale")
    places: Mapped[List["Place"]] = relationship(
        secondary=points_of_sale_and_places, back_populates="points_of_sale"
    )
    prices: WriteOnlyMapped["Price"] = relationship(back_populates="point_of_sale")

class ArticleCode(Base):
    __tablename__ = "article_codes"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)
    cards: Mapped[List["ArticleCard"]] = relationship(back_populates="code")
    prices: WriteOnlyMapped["Price"] = relationship(back_populates="article_code")
    shopping_lists: Mapped[List["ShoppingList"]] = relationship(
        secondary=shopping_list_articles, back_populates="article_codes"
    )

class ArticleBrand(Base):
    __tablename__ = "article_brands"
    id: Mapped[int] = mapped_column(primary_key=True)
    brand: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)
    cards: WriteOnlyMapped["ArticleCard"] = relationship(back_populates="brand")

class ArticleDescription(Base):
    __tablename__ = "article_descriptions"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str] = mapped_column(sa.String(128), unique=True, nullable=False)
    cards: WriteOnlyMapped["ArticleCard"] = relationship(back_populates="description")

class ArticlePackage(Base):
    __tablename__ = "article_packages"
    id: Mapped[int] = mapped_column(primary_key=True)
    package: Mapped[str] = mapped_column(sa.String(32), unique=True, nullable=False)
    cards: WriteOnlyMapped["ArticleCard"] = relationship(back_populates="package")

class ArticleCard(Base):
    __tablename__ = "article_cards"
    id: Mapped[int] = mapped_column(primary_key=True)
    brand_id: Mapped[int] = mapped_column(sa.ForeignKey("article_brands.id"), index=True)
    description_id: Mapped[int] = mapped_column(sa.ForeignKey("article_descriptions.id"), index=True)
    package_id: Mapped[int] = mapped_column(sa.ForeignKey("article_packages.id"), index=True)
    code_id: Mapped[int] = mapped_column(sa.ForeignKey("article_codes.id"), index=True)

    brand: Mapped["ArticleBrand"] = relationship(back_populates="cards")
    description: Mapped["ArticleDescription"] = relationship(back_populates="cards")
    package: Mapped["ArticlePackage"] = relationship(back_populates="cards")
    code: Mapped["ArticleCode"] = relationship(back_populates="cards")
    tags: Mapped[List["ArticleTag"]] = relationship(
        secondary=article_cards_and_article_tags, back_populates="article_cards"
    )

class Price(Base):
    __tablename__ = "prices"
    id: Mapped[int] = mapped_column(primary_key=True)
    amount: Mapped[int] = mapped_column(nullable=False, index=True)
    timestamp_id: Mapped[int] = mapped_column(sa.ForeignKey("timestamps.id"), index=True)
    point_of_sale_id: Mapped[int] = mapped_column(sa.ForeignKey("points_of_sale.id"), index=True)
    article_code_id: Mapped[int] = mapped_column(sa.ForeignKey("article_codes.id"), index=True)
    timestamp: Mapped["Timestamp"] = relationship(back_populates="prices")
    point_of_sale: Mapped["PointOfSale"] = relationship(back_populates="prices")
    article_code: Mapped["ArticleCode"] = relationship(back_populates="prices")


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(sa.String(64), index=True, unique=True)
    password_hash: Mapped[str] = mapped_column(sa.String(256))

    shopping_lists: WriteOnlyMapped["ShoppingList"] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def set_password(self, password: str) -> None:
        self.password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    def check_password(self, password: str) -> bool:
        return bcrypt.checkpw(password.encode("utf-8"), self.password_hash.encode("utf-8"))

class ShoppingList(Base):
    __tablename__ = 'shopping_lists'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(120))
    user_id: Mapped[int] = mapped_column(sa.ForeignKey('users.id'))
    user: Mapped["User"] = relationship(back_populates="shopping_lists")
    is_public: Mapped[bool] = mapped_column(sa.Boolean, default=False)
    article_codes: Mapped[List["ArticleCode"]] = relationship(
        secondary=shopping_list_articles, back_populates="shopping_lists"
    )

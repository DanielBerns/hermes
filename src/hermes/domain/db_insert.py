import logging
from collections.abc import Callable
from typing import Any, Generator
from sqlalchemy import select, insert
from sqlalchemy.orm import Session

from hermes.domain.database import (
    get_int,
    Timestamp,
    State,
    City,
    Flag,
    Business,
    Branch,
    POS,
    points_of_sale_and_places,
    Place,
    PointOfSale,
    ArticleCode,
    ArticleBrand,
    ArticleDescription,
    ArticlePackage,
    ArticleCard,
    Price
)

from hermes.domain.rows_to_db import (
    get_city_key,
    get_place_key,
    get_article_card_key
)

from hermes.core.search_table import SearchTable


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class DBInsertException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


def log_error_record(
    record: dict[str, Any]
) -> None:
    if record:
        logger.error("***Bad record***")
        for key, value in record.items():
            logger.error(f"    {key}: {value}")
    else:
        logger.error("***record is None***")


def string_to_row(ts: str) -> dict[str, Any]:
    return {
        "year": get_int(ts[0:4]),
        "month": get_int(ts[4:6]),
        "day": get_int(ts[6:8]),
        "hour": get_int(ts[8:10]),
        "minute": get_int(ts[10:12]),
        "second": get_int(ts[12:14])
    }


class DBLookup:
    def __init__(self, session: Session) -> None:
        self._session = session

        self._all_the_timestamps = SearchTable[Timestamp]("timestamp")
        self._all_the_states = SearchTable[State]("state")
        self._all_the_cities = SearchTable[City]("city_key")
        self._all_the_flags = SearchTable[Flag]("flag")
        self._all_the_businesses = SearchTable[Business]("business")
        self._all_the_branches = SearchTable[Branch]("branch")
        self._all_the_places = SearchTable[Place]("place")
        self._all_the_points_of_sale = SearchTable[PointOfSale]("point_of_sale_key")
        self._all_the_article_codes = SearchTable[ArticleCode]("article_code")
        self._all_the_article_brands = SearchTable[ArticleBrand]("brand")
        self._all_the_article_descriptions = SearchTable[ArticleDescription]("description")
        self._all_the_article_packages = SearchTable[ArticlePackage]("package")
        self._all_the_article_cards = SearchTable[ArticleCards]("article_card_key")

    @property
    def session(self) -> Session:
        return self._session

    @property
    def all_the_timestamps(self) -> SearchTable[Timestamp]:
        return self._all_the_timestamps

    @property
    def all_the_states(self) -> SearchTable[State]:
        return self._all_the_states

    @property
    def all_the_cities(self) -> SearchTable[City]:
        return self._all_the_cities

    @property
    def all_the_flags(self) -> SearchTable[Flag]:
        return self._all_the_flags

    @property
    def all_the_businesses(self) -> SearchTable[Business]:
        return self._all_the_businesses

    @property
    def all_the_branches(self) -> SearchTable[Branch]:
        return self._all_the_branches

    @property
    def all_the_places(self) -> SearchTable[Place]:
        return self._all_the_places

    @property
    def all_the_points_of_sale(self) -> SearchTable[PointOfSale]:
        return self._all_the_points_of_sale

    @property
    def all_the_article_codes(self) -> SearchTable[ArticleCode]:
        return self._all_the_article_codes

    @property
    def all_the_article_brands(self) -> SearchTable[ArticleBrand]:
        return self._all_the_article_brands

    @property
    def all_the_article_descriptions(self) -> SearchTable[ArticleDescription]:
        return self._all_the_article_descriptions

    @property
    def all_the_article_packages(self) -> SearchTable[ArticlePackage]:
        return self._all_the_article_packages

    @property
    def all_the_article_cards(self) -> SearchTable[ArticleCard]:
        return self._all_the_article_cards

    def update_cache(self) -> None:
        logger.info(f"{self.__class__.__name__}.update_cache start")
        self.get_all_the_timestamps()
        self.get_all_the_states()
        self.get_all_the_cities()
        self.get_all_the_flags()
        self.get_all_the_businesses()
        self.get_all_the_branches()
        self.get_all_the_pos()
        self.get_all_the_places()
        self.get_all_the_points_of_sale()
        self.get_all_the_article_codes()
        self.get_all_the_article_brands()
        self.get_all_the_article_descriptions()
        self.get_all_the_article_packages()
        self.get_all_the_article_cards()
        logger.info(f"{self.__class__.__name__}.update_cache stop")

    def get_all_the_states(self) -> None:
        statement = select(State)
        _all_the_states = list(self.session.scalars(statement).all())
        self.all_the_states.update({a_state.code: a_state for a_state in _all_the_states})

    def get_all_the_cities(self) -> None:
        statement = select(City)
        _all_the_cities = list(self.session.scalars(statement).all())
        self.all_the_cities.update(
            {get_city_key(a_city.state.code, a_city.name): a_city for a_city in _all_the_cities}
        )

    def get_all_the_places(self) -> None:
        statement = select(Place)
        _all_the_places = list(self.session.scalars(statement).all())
        self.all_the_places.update(
            {get_place_key(
                a_place.city.state.code,
                a_place.city.name,
                a_place.address
             ): a_place for a_place in _all_the_places}
        )

    def get_all_the_points_of_sale(self) -> None:
        statement = select(PointOfSale)
        _all_the_points_of_sale = list(self.session.scalars(statement).all())
        self.all_the_points_of_sale.update(
            {pos.code: pos for pos in _all_the_points_of_sale}
        )

    def get_all_the_timestamps(self) -> None:
        statement = select(Timestamp)
        _all_the_timestamps = list(self.session.scalars(statement).all())
        self.all_the_timestamps.update(
            {repr(ts): ts for ts in _all_the_timestamps}
        )

    def get_all_the_article_codes(self) -> None:
        statement = select(ArticleCode)
        _all_the_article_codes = list(self.session.scalars(statement).all())
        self.all_the_article_codes.update(
            {ac.code: ac for ac in _all_the_article_codes}
        )

    def get_all_the_article_cards(self) -> None:
        statement = select(ArticleCard)
        _all_the_article_cards = list(self.session.scalars(statement).all())
        self.all_the_article_cards.update(
            {get_article_card_key(ac.article_code.code, ac.brand, ac.description, ac.package): ac
             for ac in _all_the_article_cards}
        )

    def insert_timestamps(
        self,
        records_generator: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        logger.info(f"{self.__class__.__name__}.insert_timestamps start")
        try:
            bulk = [
                string_to_row(record.get("timestamp", "00000000000000"))
                for record in records_generator() if self.all_the_timestamps.search(record) == None
            ]
            if len(bulk):
                self.session.execute(insert(Timestamp), bulk)
                self.session.commit()
                self.get_all_the_timestamps()
            else:
                logger.info(f"{self.__class__.__name__}.insert_timestamps empty bulk")
        except Exception as panic:
            self.session.rollback()
            message = f"{self.__class__.__name__}.insert_timestamps failure"
            raise DBInsertException(message)
        logger.info(f"{self.__class__.__name__}.insert_timestamps done")




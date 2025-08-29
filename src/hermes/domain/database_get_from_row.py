
import logging
from typing import Any, Callable, Generator, Type, List
from abc import ABC

from sqlalchemy import insert, select, and_
from sqlalchemy.orm import Session

from hermes.core.search_table import SearchTable
from hermes.domain.rows_ops import get_int, row_to_string, timestamp_string_to_row

# Import the models and custom exception from your database definition file
from hermes.domain.database import (
    ArticleBrand,
    ArticleCard,
    ArticleCode,
    ArticleDescription,
    ArticlePackage,
    Base,
    Branch,
    Business,
    City,
    Flag,
    Place,
    PointOfSale,
    Price,
    State,
    Timestamp,
)


# Set up a logger for this module
logger = logging.getLogger(__name__)

class DatabaseGetFromRow(Exception):
    """Custom exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class DatabaseGetFromRow:
    """
    Provides a high-level API for performing Create and Read
    operations on the database, encapsulating SQLAlchemy session interactions.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def session(self) -> Session:
        return self._session

    # Private helper method to reduce boilerplate in simple getter methods
    def _get_unique_record(
        self,
        model_class: Type[Base],
        model_attribute: Any,
        row: dict[str, Any],
        row_key: str,
    ) -> Base | None:
        """
        Generic helper to query a single record by a unique attribute.

        Args:
            model_class: The ORM model class to query (e.g., State).
            model_attribute: The class attribute to filter on (e.g., State.code).
            row: The input data dictionary.
            row_key: The key in the dictionary that holds the value to query.

        Returns:
            The ORM object if found, otherwise None.
        """
        value = row.get(row_key)
        if not value:
            message = f"{self.__class__.__name__}._get_unique_record: No '{row_key}' found in row: {row_to_string(row)}"
            raise DatabaseGetFromRow(message)
        try:
            query = select(model_class).where(model_attribute == value)
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"{self.__class__.__name__}._get_unique_record: An error occurred while querying for {row_key} '{value}': {panic}"
            raise DatabaseGetFromRow(message) from panic

    def _get_all_records(
        self,
        model_class: Type[Base]
    ) -> List[Base]:
        """
        Generic helper to retrieve all records from a given table.

        Args:
            model_class: The ORM model class to query (e.g., State, City).

        Returns:
            A list of all ORM objects found in the table.
            Returns an empty list if the table is empty.

        Raises:
            DatabaseGetFromRow: If an error occurs during the database query.
        """
        try:
            # Construct a simple query to select all records from the table.
            query = select(model_class)
            # Execute the query and get all scalar results (the ORM objects).
            # The .scalars().all() method is the standard way to get a list of objects.
            return self.session.execute(query).scalars().all()
        except Exception as panic:
            # Wrap any potential database error in our custom exception.
            table_name = model_class.__tablename__
            message = f"{self.__class__.__name__}._get_all_records: An error occurred while querying all records from '{table_name}': {panic}"
            raise DatabaseGetFromRow(message) from panic

    def get_timestamp(self, row: dict[str, Any]) -> Timestamp | None:
        """
        Queries for a Timestamp record by its components (year, month, day, etc.).
        This method is custom due to the composite nature of the query.
        """
        timestamp_string = row.get("timestamp")
        if not timestamp_string:
            message = f"{self.__class__.__name__}.get_timestamp: No 'timestamp' found in row: {row_to_string(row)}"
            raise DatabaseGetFromRow(message)
        try:
            ts_target = Timestamp.from_string(timestamp_string)
            query = select(Timestamp).where(
                Timestamp.year == ts_target.year,
                Timestamp.month == ts_target.month,
                Timestamp.day == ts_target.day,
                Timestamp.hour == ts_target.hour,
                Timestamp.minute == ts_target.minute,
                Timestamp.second == ts_target.second,
            )
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"{self.__class__.__name__}.get_timestamp: An error occurred while querying timestamp '{timestamp_string}': {panic}"
            raise DatabaseGetFromRow(message) from panic

    def get_city(self, row: dict[str, Any]) -> City | None:
        """
        Queries the database for a City record by its name and the code of its state.
        This composite lookup is necessary because city names are not unique across
        different states.

        Args:
            row: A dict[str, Any] with 'city' and 'state' keys.

        Returns:
            The City object if found, otherwise None.
            Raises an exception if multiple cities are found with the same name
            and state code, which would indicate a data integrity issue.
        """
        city_name = row.get("city")
        state_code = row.get("state")

        if not city_name or not state_code:
            message = f"{self.__class__.__name__}.get_city: Missing 'city' or 'state_code' in row: {row_to_string(row)}"
            raise DatabaseGetFromRow(message)

        try:
            # Construct a query with a join to the State table to filter by state_code.
            # Using the 'City.state' relationship makes the join implicit and clean.
            query = (
                select(City)
                .join(City.state)
                .where(
                    and_(
                        City.name == city_name,
                        State.code == state_code
                    )
                )
            )

            # .scalar_one_or_none() is ideal for ensuring a unique result.
            result = self.session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            message = (
                f"{self.__class__.__name__}.get_city: An error occurred while querying for city '{city_name}' "
                f"in state '{state_code}': {panic}"
            )
            raise DatabaseGetFromRow(message) from panic

    # Refactored getter methods to use the generic helper
    def get_state(self, row: dict[str, Any]) -> State | None:
        return self._get_unique_record(State, State.code, row, "code")

    def get_flag(self, row: dict[str, Any]) -> Flag | None:
        return self._get_unique_record(Flag, Flag.name, row, "flag")

    def get_business(self, row: dict[str, Any]) -> Business | None:
        return self._get_unique_record(Business, Business.name, row, "business")

    def get_branch(self, row: dict[str, Any]) -> Branch | None:
        return self._get_unique_record(Branch, Branch.name, row, "branch")

    def get_place(self, row: dict[str, Any]) -> Place | None:
        return self._get_unique_record(Place, Place.address, row, "address")

    def get_point_of_sale(self, row: dict[str, Any]) -> PointOfSale | None:
        return self._get_unique_record(PointOfSale, PointOfSale.code, row, "code")

    def get_article_code(self, row: dict[str, Any]) -> ArticleCode | None:
        return self._get_unique_record(ArticleCode, ArticleCode.article_code, row, "article_code")

    def get_article_brand(self, row: dict[str, Any]) -> ArticleBrand | None:
        return self._get_unique_record(ArticleBrand, ArticleBrand.article_brand, row, "article_brand")

    def get_article_description(self, row: dict[str, Any]) -> ArticleDescription | None:
        return self._get_unique_record(
            ArticleDescription, ArticleDescription.article_description, row, "article_description"
        )

    def get_article_package(self, row: dict[str, Any]) -> ArticlePackage | None:
        return self._get_unique_record(
            ArticlePackage, ArticlePackage.article_package, row, "article_package"
        )

    def get_article_card(self, row: dict[str, Any]) -> ArticleCard | None:
        """
        Queries for an ArticleCard record by its foreign keys.
        This method is custom because it relies on multiple related entities.
        """
        try:
            # First, retrieve the related entities.
            # Ensure all components required for the card exist.
            # If a component is not found, it's not an error, just means the card can't exist yet.
            article_brand = self.get_article_brand(row)
            if article_brand is None:
                return None
            article_description = self.get_article_description(row)
            if article_description is None:
                return None
            article_package = self.get_article_package(row)
            if article_package is None:
                return None
            article_code = self.get_article_code(row)
            if article_code is None:
                return None

            # Construct the query using the IDs of the found components.
            query = select(ArticleCard).where(
                ArticleCard.brand_id == article_brand.id,
                ArticleCard.description_id == article_description.id,
                ArticleCard.package_id == article_package.id,
                ArticleCard.code_id == article_code.id,
            )
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"An error occurred while querying for article_card {row_to_string(row)}: {panic}"
            raise DatabaseGetFromRow(message) from panic


import pdb
import logging
from collections.abc import Callable
from typing import Any, Generator, Type, List
from abc import ABC

from sqlalchemy import insert, select, and_
from sqlalchemy.orm import Session

from hermes.core.search_table import SearchTable
from hermes.core.tree_store import Store
from hermes.domain.rows_ops import get_int, row_to_string, timestamp_string_to_row
from hermes.domain.sample_reader import SampleReader
# from hermes.domain.sample_processor import SampleProcessor

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

class DatabaseFromSampleException(Exception):
    """Custom exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)



CODES_AND_STATES = {
  "ar-a": ("ar-a", "Salta"),
  "salta": ("ar-a", "Salta"),
  "ar-b": ("ar-b", "Buenos Aires"),
  "buenos aires": ("ar-b","Buenos Aires"),
  "ar-c": ("ar-c", "CABA"),
  "caba": ("ar-c", "CABA"),
  "capital federal": ("ar-c", "CABA"),
  "ar-d": ("ar-d", "San Luis"),
  "san luis": ("ar-d", "San Luis"),
  "ar-e": ("ar-e", "Entre Rios"),
  "ar-f": ("ar-f", "La Rioja"),
  "la rioja": ("ar-f", "La Rioja"),
  "ar-g": ("ar-g", "Santiago del Estero"),
  "santiago del estero": ("ar-g", "Santiago del Estero"),
  "ar-h": ("ar-h", "Chaco"),
  "chaco": ("ar-h","Chaco"),
  "ar-j": ("ar-j", "San Juan"),
  "ar-k": ("ar-k", "Catamarca"),
  "catamarca" : ("ar-k","Catamarca"),
  "ar-l": ("ar-l", "La Pampa"),
  "la pampa": ("ar-l", "La Pampa"),
  "ar-m": ("ar-m", "Mendoza"),
  "mendoza": ("ar-m", "Mendoza"),
  "ar-n": ("ar-n", "Misiones"),
  "misiones": ("ar-n", "Misiones"),
  "ar-p": ("ar-p", "Formosa"),
  "formosa": ("ar-p", "Formosa"),
  "ar-q": ("ar-q", "Neuquén"),
  "neuquen": ("ar-q", "Neuquén"),
  "ar-r": ("ar-r", "Río Negro"),
  "rio negro": ("ar-r", "Río Negro"),
  "ar-s": ("ar-s", "Santa Fé"),
  "santa fe": ("ar-s", "Santa Fe"),
  "ar-t": ("ar-t", "Tucumán"),
  "tucuman": ("ar-t", "Tucumán"),
  "ar-u": ("ar-u", "Chubut"),
  "ar-v": ("ar-v", "Tierra del Fuego"),
  "ar-w": ("ar-w", "Corrientes"),
  "ar-x": ("ar-x", "Córdoba"),
  "cordoba": ("ar-x", "Córdoba"),
  "ar-y": ("ar-y", "Jujuy"),
  "jujuy": ("ar-y", "Jujuy"),
  "ar-z": ("ar-z", "Santa Cruz"),
  "error": ("xxxx", "Error")
}

class DatabaseFromSample:
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
            raise DatabaseFromSampleException(message)
        try:
            query = select(model_class).where(model_attribute == value)
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"{self.__class__.__name__}._get_unique_record: An error occurred while querying for {row_key} '{value}': {panic}"
            raise DatabaseFromSampleException(message) from panic

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
            DatabaseFromSampleException: If an error occurs during the database query.
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
            raise DatabaseFromSampleException(message) from panic

    def get_timestamp(self, row: dict[str, Any]) -> Timestamp | None:
        """
        Queries for a Timestamp record by its components (year, month, day, etc.).
        This method is custom due to the composite nature of the query.
        """
        timestamp_string = row.get("timestamp")
        if not timestamp_string:
            message = f"{self.__class__.__name__}.get_timestamp: No 'timestamp' found in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
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
            raise DatabaseFromSampleException(message) from panic

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
        city_name = row.get("name")
        if not city_name:
            message = f"{self.__class__.__name__}.get_city: Missing 'name' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)

        state_id = row.get("state_id")
        if not state_id:
            message = f"{self.__class__.__name__}.get_city: Missing 'state_id' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)

        try:
            # Construct a query with a join to the State table to filter by state_code.
            # Using the 'City.state' relationship makes the join implicit and clean.
            query = (
                select(City)
                .join(City.state)
                .where(
                    and_(
                        City.name == city_name,
                        State.id == state_id
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
            raise DatabaseFromSampleException(message) from panic

    def get_place(self, row: dict[str, Any]) -> Place | None:
        """
        Queries the database for a City record by its name and the code of its state.
        This composite lookup is necessary because city names are not unique across
        different states.

        Args:
            row: A dict[str, Any] with 'address' and 'city' keys.

        Returns:
            The Place object if found, otherwise None.
            Raises an exception if multiple places are found with the same name
            and city, which would indicate a data integrity issue.
        """
        address = row.get("address")
        if not address:
            message = f"{self.__class__.__name__}.get_address: Missing 'address' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)

        city_id = row.get("city_id")
        if not city_id:
            message = f"{self.__class__.__name__}.get_city: Missing 'city_id' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)

        try:
            # Construct a query with a join to the State table to filter by state_code.
            # Using the 'City.state' relationship makes the join implicit and clean.
            query = (
                select(Place)
                .join(Place.city)
                .where(
                    and_(
                        Place.address == address,
                        City.id == city_id
                    )
                )
            )

            # .scalar_one_or_none() is ideal for ensuring a unique result.
            result = self.session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            message = (
                f"{self.__class__.__name__}.get_place: An error occurred while querying for place '{address}' "
                f"in city_id '{city_id}': {panic}"
            )
            raise DatabaseFromSampleException(message) from panic

    # Refactored getter methods to use the generic helper
    def get_state(self, row: dict[str, Any]) -> State | None:
        return self._get_unique_record(State, State.code, row, "code")

    def get_flag(self, row: dict[str, Any]) -> Flag | None:
        return self._get_unique_record(Flag, Flag.flag, row, "flag")

    def get_business(self, row: dict[str, Any]) -> Business | None:
        return self._get_unique_record(Business, Business.business, row, "business")

    def get_branch(self, row: dict[str, Any]) -> Branch | None:
        return self._get_unique_record(Branch, Branch.branch, row, "branch")

    # def get_place(self, row: dict[str, Any]) -> Place | None:
    #     return self._get_unique_record(Place, Place.address, row, "address")

    def get_point_of_sale(self, row: dict[str, Any]) -> PointOfSale | None:
        return self._get_unique_record(PointOfSale, PointOfSale.code, row, "code")

    def get_article_code(self, row: dict[str, Any]) -> ArticleCode | None:
        return self._get_unique_record(ArticleCode, ArticleCode.code, row, "code")

    def get_article_brand(self, row: dict[str, Any]) -> ArticleBrand | None:
        return self._get_unique_record(ArticleBrand, ArticleBrand.brand, row, "brand")

    def get_article_description(self, row: dict[str, Any]) -> ArticleDescription | None:
        return self._get_unique_record(
            ArticleDescription, ArticleDescription.description, row, "description"
        )

    def get_article_package(self, row: dict[str, Any]) -> ArticlePackage | None:
        return self._get_unique_record(
            ArticlePackage, ArticlePackage.package, row, "package"
        )

    def get_article_card(self, row: dict[str, Any]) -> ArticleCard | None:
        """
        Queries for an ArticleCard record by its foreign keys.
        This method is custom because it relies on multiple related entities.
        """
        try:
            # First, retrieve the related entities.
            article_brand_id = row.get("brand_id")
            article_description_id = row.get("description_id")
            article_package_id = row.get("package_id")
            article_code_id = row.get("code_id")

            # Ensure all components required for the card exist.
            if not all([article_brand_id, article_description_id, article_package_id, article_code_id]):
                # This indicates that one of the components (brand, description, etc.) is not yet in the DB.
                # It's not an error, just means the card can't exist yet.
                return None

            # Construct the query using the IDs of the found components.
            query = select(ArticleCard).where(
                ArticleCard.brand_id == article_brand_id,
                ArticleCard.description_id == article_description_id,
                ArticleCard.package_id == article_package_id,
                ArticleCard.code_id == article_code_id,
            )
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"An error occurred while querying for article_card {row_to_string(row)}: {panic}"
            raise DatabaseFromSampleException(message) from panic

    # --- Data Preparation Methods (`*_from_row`) ---

    def timestamp_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp_string = row.get("timestamp")
        if timestamp_string is None:
            message = f"{self.__class__.__name__}.timestamp_from_row: Missing 'timestamp' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return timestamp_string_to_row(timestamp_string)

    def state_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        state = row.get("state")
        if not state:
            message = f"{self.__class__.__name__}.state_from_row: Missing 'state' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        fixed = CODES_AND_STATES.get(state)
        if fixed:
            code, name = fixed
        else:
            code, name = "xxxx", "Error"
        return {"code": code, "name": name}

    def city_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        state = row.get("state")
        if state is None:
            message = f"{self.__class__.__name__}.city_from_row: Missing 'state' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        fixed = CODES_AND_STATES.get(state)
        if fixed:
            state_code, _ = fixed
        else:
            state_code, _ = "xxxx", "Error"
        city = row.get("city")
        if city is None:
            message = f"{self.__class__.__name__}.city_from_row: Missing 'city' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)

        state = self.get_state({"code": state_code})
        if state is None:
            message = f"{self.__class__.__name__}.city_from_row: Missing 'state' in database with state_code {state_code}"
            raise DatabaseFromSampleException(message)
        return {"name": city, "state_id": state.id}

    def place_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        address = row.get("address")
        if address is None:
            message = f"{self.__class__.__name__}.place_from_row: Missing 'address' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        city_row = self.city_from_row(row)
        city = self.get_city(city_row)
        if not city:
            message = f"{self.__class__.__name__}.place_from_row: no city in database: {row_to_string(city_row)}"
            raise DatabaseFromSampleException(message)
        return {"address": address, "city_id": city.id}

    def flag_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        flag = row.get("flag")
        if flag is None:
            message = f"{self.__class__.__name__}.flag_from_row: Missing 'flag' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"flag": flag}

    def business_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        business = row.get("business")
        if business is None:
            message = f"{self.__class__.__name__}.business_from_row: Missing 'business' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"business": business}

    def branch_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        branch = row.get("branch")
        if branch is None:
            message = f"{self.__class__.__name__}.branch_from_row: Missing 'branch' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"branch": branch}


    def point_of_sale_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        point_of_sale_key = row.get("point_of_sale_key")
        if point_of_sale_key is None:
            message = f"{self.__class__.__name__}.point_of_sale_from_row: Missing 'point_of_sale_key' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        flag = self.get_flag(row)
        if flag is None:
            message = f"{self.__class__.__name__}.point_of_sale_from_row: Missing 'flag' in database: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        business = self.get_business(row)
        if business is None:
            message = f"{self.__class__.__name__}.point_of_sale_from_row: Missing 'business' in database: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        branch = self.get_branch(row)
        if branch is None:
            message = f"{self.__class__.__name__}.point_of_sale_from_row: Missing 'branch' in database: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {
            "code": point_of_sale_key,
            "flag_id": flag.id,
            "business_id": business.id,
            "branch_id": branch.id
        }


    def point_of_sale_at_place_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        # pdb.set_trace()
        place_row = self.place_from_row(row)
        point_of_sale_row = self.point_of_sale_from_row(row)
        place = self.get_place(place_row)
        if place is None:
            message = f"{self.__class__.__name__}.point_of_sale_at_place_from_row: no 'place' in database - {row_to_string(place_row)}"
            raise DatabaseFromSampleException(message)
        point_of_sale = self.get_point_of_sale(point_of_sale_row)
        if point_of_sale is None:
            message = f"{self.__class__.__name__}.point_of_sale_at_place_from_row: no 'point_of_sale' in database - {row_to_string(point_of_sale_row)}"
            raise DatabaseFromSampleException(message)
        return {
            "place": place,
            "point_of_sale": point_of_sale
        }

    def article_code_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        code = row.get("article_code")
        if code is None:
            message = f"{self.__class__.__name__}.article_code_from_row: Missing 'article_code' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"code": code}

    def article_brand_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        brand = row.get("brand")
        if brand is None:
            message = f"{self.__class__.__name__}.article_brand_from_row: Missing 'brand' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"brand": brand}

    def article_description_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        description = row.get("description")
        if description is None:
            message = f"{self.__class__.__name__}.article_description_from_row: Missing 'description' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"description": description}

    def article_package_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        package = row.get("package")
        if package is None:
            message = f"{self.__class__.__name__}.article_package_from_row: Missing 'package' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        return {"package": package}

    def article_card_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        brand_row = self.article_brand_from_row(row)
        description_row = self.article_description_from_row(row)
        package_row = self.article_package_from_row(row)
        article_code_row = self.article_code_from_row(row)
        brand = self.get_article_brand(brand_row)
        if brand is None:
            message = f"{self.__class__.__name__}.article_card_from_row: Missing brand in database {row_to_string(brand_row)}"
            raise DatabaseFromSampleException(message)
        description = self.get_article_description(description_row)
        if description is None:
            message = f"{self.__class__.__name__}.article_card_from_row: Missing description in database {row_to_string(description_row)}"
            raise DatabaseFromSampleException(message)
        package = self.get_article_package(package_row)
        if package is None:
            message = f"{self.__class__.__name__}.article_card_from_row: Missing package in database {row_to_string(package_row)}"
            raise DatabaseFromSampleException(message)
        article_code = self.get_article_code(article_code_row)
        if article_code is None:
            message = f"{self.__class__.__name__}.article_card_from_row: Missing article_code in database {row_to_string(article_code_row)}"
            raise DatabaseFromSampleException(message)
        return {
            "brand_id": brand.id,
            "description_id": description.id,
            "package_id": package.id,
            "code_id": article_code.id,
        }

    def price_from_row(
        self,
        row: dict[str, Any],
        timestamp: Timestamp
    ) -> dict[str, Any]:
        try:
           amount = int(row.get("price"))
        except Exception as panic:
            message = f"{self.__class__.__name__}.price_from_row: Missing 'price' from {row_to_string(row)}"
            raise DatabaseFromSampleException(message) from panic

        article_code_row = self.article_code_from_row(row)
        article_code = self.get_article_code(article_code_row)
        if article_code is None:
            message = f"{self.__class__.__name__}.price_from_row: Missing article_code in database {row_to_string(article_code_row)}"
            raise DatabaseFromSampleException(message)
        point_of_sale_key = row.get("point_of_sale_key")
        point_of_sale = self.get_point_of_sale({"code": point_of_sale_key})
        if point_of_sale is None:
            message = f"{self.__class__.__name__}.price_from_row: Missing point_of_sale in database {row_to_string(point_of_sale_row)}"
            raise DatabaseFromSampleException(message)
        return {
            "amount": amount,
            "timestamp_id": timestamp.id,
            "article_code_id": article_code.id,
            "point_of_sale_id": point_of_sale.id,
        }

    # --- Bulk Insertion Methods ---

    def _execute_bulk_insert(
        self,
        model_class: Type[Base],
        bulk_data: list[dict[str, Any]],
        context_name: str,
    ) -> None:
        """
        Private helper to execute a bulk insert statement and handle transactions.

        Args:
            model_class: The ORM model to insert data into.
            bulk_data: A list of dictionaries, where each dict represents a row.
            context_name: A string name for logging/error messages (e.g., 'states').
        """
        if not bulk_data:
            logger.warning(f"{self.__class__.__name__}._execute_bulk_insert: Bulk insert for '{context_name}' skipped: empty bulk.")
            return

        try:
            self.session.execute(insert(model_class), bulk_data)
            self.session.commit()
            logger.info(f"{self.__class__.__name__}._execute_bulk_insert: Successfully inserted {len(bulk_data)} new records into '{context_name}'.")
        except Exception as panic:
            self.session.rollback()
            message = f"{self.__class__.__name__}._execute_bulk_insert: Bulk insert failed for '{context_name}': {panic}"
            raise DatabaseFromSampleException(message) from panic

    def insert_timestamps(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.timestamp_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(Timestamp, bulk, "timestamps")

    def insert_states(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(State, bulk, "states")

    def insert_cities(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(City, bulk, "cities")

    def insert_places(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(Place, bulk, "places")

    def insert_flags(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(Flag, bulk, "flags")

    def insert_businesses(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(Business, bulk, "businesses")

    def insert_branches(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(Branch, bulk, "branches")

    def insert_points_of_sale(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(PointOfSale, bulk, "points_of_sale")

    def insert_point_of_sale_at_place(
        self,
        generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        """
        Creates associations between multiple PointsOfSale and Places in a single
        database transaction.

        This is a highly efficient method for populating the association table from
        a large dataset. It will skip rows where the PointOfSale or Place cannot
        be found, or where the association already exists, and log them instead
        of failing the entire operation.

        Args:
            generate_rows: A function that returns a generator of dictionaries.
                           Each dictionary must contain the keys to identify a
                           PointOfSale and a Place.
        """
        # pdb.set_trace()
        rows_count = 0
        success_count = 0
        skipped_count = 0
        error_count = 0

        try:
            for row in generate_rows():
                rows_count += 1
                place = row.get("place")
                point_of_sale = row.get("point_of_sale")
                if not point_of_sale or not place:
                    logger.warning(
                        f"Skipping unexpected bad row"
                    )
                    error_count += 1
                    continue

                # Check if the association already exists
                if place in point_of_sale.places:
                    skipped_count += 1
                    continue

                # Stage the new association. SQLAlchemy tracks this change.
                point_of_sale.places.append(place)
                success_count += 1

            # After the loop, if any changes were staged, commit them all at once.
            if success_count > 0:
                self.session.commit()

                # Log a final summary
                logger.info(
                    f"Summary: {rows_count} rows generated,"
                    f"{success_count} new associations created, "
                    f"{skipped_count} duplicates skipped, "
                    f"{error_count} rows with errors."
                )

        except Exception as panic:
            # If any error occurs (e.g., during the commit), roll back everything.
            self.session.rollback()
            message = f"Bulk association failed: {panic}"
            raise DatabaseFromSampleException(message) from panic

    def insert_article_codes(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(ArticleCode, bulk, "article_codes")

    def insert_article_brands(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(ArticleBrand, bulk, "article_brands")

    def insert_article_descriptions(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(ArticleDescription, bulk, "article_descriptions")

    def insert_article_packages(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(ArticlePackage, bulk, "article_packages")

    def insert_article_cards(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(ArticleCard, bulk, "article_cards")

    def insert_prices(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [row for row in generate_rows()]
        self._execute_bulk_insert(Price, bulk, "prices")

    # --- Read and Process a Sample

    def read_and_process_sample(
        self,
        store: Store
    ) -> None:
        reader = SampleReader(store)
        # processor = SampleProcessor()
        reader_timestamp_row = reader.timestamp_row()
        if self.get_timestamp(reader_timestamp_row):
            return

        sample_points_of_sale = [
            sample_row for sample_row in reader.points_of_sale()
        ]

        sample_articles_by_point_of_sale = [
            sample_row for sample_row in reader.articles_by_point_of_sale()
        ]

        def generate_timestamps() -> Generator[dict[str, Any], None, None]:
            yield from [reader_timestamp_row]

        self.insert_timestamps(generate_timestamps)

        current_timestamp = self.get_timestamp(reader_timestamp_row)
        if current_timestamp is None:
            message = f"{self.__class__.__name__}.read_and_process_sample: current_timestamp is None. Check {row_to_string(reader_timestamp_row)}"
            raise DatabaseFromSampleException(message)

        # #   () # After inserting timestamps
        def generate_states() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["code", "name"])
            for a_row in sample_points_of_sale:
                # #   ()
                candidate = self.state_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_state(candidate):
                        continue
                    yield candidate

        self.insert_states(generate_states)

        # #   () # Cities
        def generate_cities() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["name", "state_id"])
            for a_row in sample_points_of_sale:
                candidate = self.city_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_city(candidate):
                        continue
                    yield candidate

        self.insert_cities(generate_cities)

        # #   () # Places
        def generate_places() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["address", "city_id"])
            for a_row in sample_points_of_sale:
                candidate = self.place_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_place(candidate):
                        continue
                    yield candidate

        self.insert_places(generate_places)

        # #   () # Flags
        def generate_flags() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["flag"])
            for a_row in sample_points_of_sale:
                candidate = self.flag_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_flag(candidate):
                        continue
                    yield candidate

        self.insert_flags(generate_flags)

        # #   () # Businesses
        def generate_businesses() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["business"])
            for a_row in sample_points_of_sale:
                candidate = self.business_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_business(candidate):
                        continue
                    yield candidate

        self.insert_businesses(generate_businesses)

        # #   () # Branches
        def generate_branches() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["branch"])
            for a_row in sample_points_of_sale:
                candidate = self.branch_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_branch(candidate):
                        continue
                    yield candidate

        self.insert_branches(generate_branches)

        # #   () # Points of sale
        def generate_points_of_sale() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["code", "flag", "business", "branch"])
            for a_row in sample_points_of_sale:
                candidate = self.point_of_sale_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_point_of_sale(candidate):
                        continue
                    yield candidate

        self.insert_points_of_sale(generate_points_of_sale)

        def generate_point_of_sale_at_place() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["place", "point_of_sale"])
            for a_row in sample_points_of_sale:
                candidate = self.point_of_sale_at_place_from_row(a_row)
                if search_table.insert(candidate):
                    yield candidate

        # pdb.set_trace()
        self.insert_point_of_sale_at_place(generate_point_of_sale_at_place)

        # #   () # Codes
        def generate_article_codes() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["code"])
            for a_row in sample_articles_by_point_of_sale:
                candidate = self.article_code_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_article_code(candidate):
                        continue
                    yield candidate

        self.insert_article_codes(generate_article_codes)

        # #   () # Brands
        def generate_article_brands() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["brand"])
            for a_row in sample_articles_by_point_of_sale:
                candidate = self.article_brand_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_article_brand(candidate):
                        continue
                    yield candidate

        self.insert_article_brands(generate_article_brands)

        # #   () # Descriptions
        def generate_article_descriptions() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["description"])
            for a_row in sample_articles_by_point_of_sale:
                candidate = self.article_description_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_article_description(candidate):
                        continue
                    yield candidate

        self.insert_article_descriptions(generate_article_descriptions)

        # #   () # Packages
        def generate_article_packages() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["package"])
            for a_row in sample_articles_by_point_of_sale:
                candidate = self.article_package_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_article_package(candidate):
                        continue
                    yield candidate

        self.insert_article_packages(generate_article_packages)

        # #   () # Cards
        def generate_article_cards() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["brand_id", "description_id", "package_id", "code_id"])
            for a_row in sample_articles_by_point_of_sale:
                candidate = self.article_card_from_row(a_row)
                if search_table.insert(candidate):
                    if self.get_article_card(candidate):
                        continue
                    yield candidate

        self.insert_article_cards(generate_article_cards)

        #   () # Prices
        def generate_prices() -> Generator[dict[str, Any], None, None]:
            for a_row in sample_articles_by_point_of_sale:
                yield self.price_from_row(a_row, current_timestamp)

        self.insert_prices(generate_prices)

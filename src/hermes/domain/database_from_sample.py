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
  "jujuy" ("ar-y", "Jujuy"),
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
        city_name = row.get("city")
        state_code = row.get("state")

        if not city_name or not state_code:
            message = f"{self.__class__.__name__}.get_city: Missing 'city' or 'state_code' in row: {row_to_string(row)}"
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
            raise DatabaseFromSampleException(message) from panic

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
        return self._get_unique_record(ArticleCode, ArticleCode.article_code, row, "code")

    def get_article_brand(self, row: dict[str, Any]) -> ArticleBrand | None:
        return self._get_unique_record(ArticleBrand, ArticleBrand.article_brand, row, "brand")

    def get_article_description(self, row: dict[str, Any]) -> ArticleDescription | None:
        return self._get_unique_record(
            ArticleDescription, ArticleDescription.article_description, row, "description"
        )

    def get_article_package(self, row: dict[str, Any]) -> ArticlePackage | None:
        return self._get_unique_record(
            ArticlePackage, ArticlePackage.article_package, row, "package"
        )

    def get_article_card(self, row: dict[str, Any]) -> ArticleCard | None:
        """
        Queries for an ArticleCard record by its foreign keys.
        This method is custom because it relies on multiple related entities.
        """
        try:
            # First, retrieve the related entities.
            article_brand = self.get_article_brand(row)
            article_description = self.get_article_description(row)
            article_package = self.get_article_package(row)
            article_code = self.get_article_code(row)

            # Ensure all components required for the card exist.
            if not all([article_brand, article_description, article_package, article_code]):
                # This indicates that one of the components (brand, desc, etc.) is not yet in the DB.
                # It's not an error, just means the card can't exist yet.
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
        code = row.get("code")
        name = row.get("name")
        if not code or not name:
            message = f"{self.__class__.__name__}.timestamp_from_row: Missing 'code' or 'name' for state in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
        fixed = CODES_AND_STATES.get(code)
        if fixed:
            code, name = fixed
        else:
            code, name = "xxxx", "Error"
        return {"code": code, "name": name}

    def city_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        state_code = row.get("state")
        if state_code is None:
            message = f"{self.__class__.__name__}.city_from_row: Missing 'state' in row: {row_to_string(row)}"
            raise DatabaseFromSampleException(message)
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
           amount = int(row.get("price")))
        except Exception as panic:
            message = f"{self.__class__.__name__}.price_from_row: Missing 'price' from {row_to_string(row)}"
            raise DatabaseFromSampleException(message) from panic

        article_code_row = self.article_code_from_row(row)
        article_code = self.get_article_code(article_code_row)
        if article_code is None:
            message = f"{self.__class__.__name__}.price_from_row: Missing article_code in database {row_to_string(article_code_row)}"
            raise DatabaseFromSampleException(message)
        point_of_sale_row = self.point_of_sale_from_row(row)
        point_of_sale = self.get_point_of_sale(point_of_sale_row)
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
        bulk = [self.state_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(State, bulk, "states")

    def insert_cities(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.city_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(City, bulk, "cities")

    def insert_places(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.place_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(Place, bulk, "places")

    def insert_flags(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.flag_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(Flag, bulk, "flags")

    def insert_businesses(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.business_from_row(row, all_the_businesses) for row in generate_rows()]
        self._execute_bulk_insert(Business, bulk, "businesses")

    def insert_branches(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.branch_from_row(row, all_the_branches) for row in generate_rows()]
        self._execute_bulk_insert(Branch, bulk, "branches")

    def insert_article_codes(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.article_code_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(ArticleCode, bulk, "article_codes")

    def insert_article_brands(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.article_brand_from_row(row, all_the_article_brands) for row in generate_rows()]
        self._execute_bulk_insert(ArticleBrand, bulk, "article_brands")

    def insert_article_descriptions(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        bulk = [
            self.article_description_from_row(row, all_the_article_descriptions)
            for row in generate_rows()
            if all_the_article_descriptions.search(row) is None
        ]
        self._execute_bulk_insert(ArticleDescription, bulk, "article_descriptions")

    def insert_article_packages(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        bulk = [self.article_package_from_row(row) for row in generate_rows()]
        self._execute_bulk_insert(ArticlePackage, bulk, "article_packages")

    def insert_article_cards(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        bulk = [self.article_card_from_row(row, all_the_article_cards) for row in generate_rows()]
        self._execute_bulk_insert(ArticleCard, bulk, "article_cards")

    def insert_prices(
        self,
        generate_rows: Callable[[], Generator[dict[str, Any], None, None]],
        timestamp: Timestamp,
    ) -> None:
        # Note: Prices are always inserted as new records for a given timestamp
        # and are not checked for pre-existence, which is the intended behavior.
        bulk = [self.price_from_row(row, timestamp) for row in generate_rows()]
        self._execute_bulk_insert(Price, bulk, "prices")

    # --- Read and Process a Sample

    def read_and_process_sample(
        self,
        store: Store
    ) -> None:
        reader = SampleReader(store)
        processor = SampleProcessor()
        current_timestamp = reader.timestamp()
        if self.get_timestamp(current_timestamp):
            return

        sample_points_of_sale = [
            processor.point_of_sale_from_row(sample_row)
            for sample_row in reader.points_of_sale()
        ]

        sample_articles_by_point_of_sale = [
            processor.point_of_sale_from_row(sample_row)
            for sample_row in reader.articles_by_point_of_sale()
        ]

        def generate_timestamps() -> Generator[dict[str, Any], None, None]:
            yield from [current_timestamp]

        self.insert_timestamps(generate_timestamps)

        def generate_states() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["code", "name"])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.state_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_state(candidate):
                    continue
                yield candidate

        self.insert_states(generate_states)

        def generate_cities() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["name", "state_id"])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.city_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_city(candidate):
                    continue
                yield candidate

        self.insert_cities(generate_cities)

        def generate_places() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["address", "city_id"])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.place_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_place(candidate):
                    continue
                yield candidate

        self.insert_places(generate_places)

        def generate_businesses() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["name"])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.business_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_business(candidate):
                    continue
                yield candidate

        self.insert_businesses(generate_businesses)

        def generate_branches() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(["name"])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.branch_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_branch(candidate):
                    continue
                yield candidate

        self.insert_branches(generate_businesses)

        def generate_points_of_sale() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable([])
            for a_row in sample_points_of_sale:
                search_table.insert(
                    self.point_of_sale_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_point_of_sale(candidate):
                    continue
                yield candidate

        self.insert_points_of_sale(generate_points_of_sale)

        def generate_brands() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(ArticleBrand.labels())
            for a_row in sample_articles_by_point_of_sale:
                search_table.insert(
                    self.article_brand_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_article_brand(candidate):
                    continue
                yield candidate

        self.insert_article_brands(generate_article_brands)

        def generate_descriptions() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(ArticleDescription.labels())
            for a_row in sample_articles_by_point_of_sale:
                search_table.insert(
                    self.article_description_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_article_description(candidate):
                    continue
                yield candidate

        self.insert_article_descriptions(generate_article_descriptions)

        def generate_packages() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(ArticlePackage.labels())
            for a_row in sample_articles_by_point_of_sale:
                search_table.insert(
                    self.article_package_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_article_package(candidate):
                    continue
                yield candidate

        self.insert_article_packages(generate_article_packages)

        def generate_article_cards() -> Generator[dict[str, Any], None, None]:
            search_table = SearchTable(ArticleCard.labels())
            for a_row in sample_articles_by_point_of_sale:
                search_table.insert(
                    self.article_card_from_row(a_row)
                )
            for candidate in search_table.iterate():
                if self.get_article_card(candidate):
                    continue
                yield candidate

        self.insert_article_cards(generate_article_cards)

        def generate_prices() -> Generator[dict[str, Any], None, None]:
            for a_row in sample_articles_by_point_of_sale:
                yield self.price_from_row(a_row, current_timestamp)

        self.insert_prices(generate_prices)

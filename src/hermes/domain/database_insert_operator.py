
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

class DatabaseInsertException(Exception):
    """Custom exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class DBRowsGenerator(ABC):
    def __init__(
        self,
        store: Store,
        reader: SampleReader,
        identifier: str,
        table: dict[str, bool]
    ) -> None:
        self._store = store
        self._reader = reader
        self._identifier = identifier
        self._table: dict[str, bool] = table

    @property
    def store(self) -> Store:
        return self._store

    @property
    def reader(self) -> SampleReader:
        return self._reader

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def table(self) -> dict[str, bool]:
        return self._table

    @abstractmethod
    def iterate(self) -> Generator[dict[str, Any], None, None]:
        ...


class DBTimestampsGenerator(DBRowsGenerator):
    def __init__(
        self,
        store: Store,
        reader: SampleReader,
        identifier: str,
        table: dict[str, Any]
    ) -> None:
        super().__init__(store, reader, "timestamps", table)

    def iterate(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.iterate: start")
        store_timestamp = self.store.timestamp
        test = not self.table.get(store_timestamp, False)
        if test:
            self.table[store_timestamp] = True
            yield from [{"timestamp": store_timestamp}]
        else:
            message = f"{self.__class__.__name__}.iterate: check {store_timestamp}"
            raise DatabaseInsertException(message)
        logger.info(f"{classname(self)}.iterate: done {len(self.table)}")

class DBStatesGenerator(DBRowsGenerator):

    CODES_AND_STATES = {
      "ar-a": "ar-a", "Salta",
      "ar-b": "ar-b", "Buenos Aires",
      "ar-c": "ar-c", "CABA",
      "ar-d": "ar-d", "San Luis",
      "ar-e": "ar-e", "Entre Rios",
      "ar-f": "ar-f", "La Rioja",
      "ar-g": "ar-g", "Santiago del Estero",
      "ar-h": "ar-h", "Chaco",
      "ar-j": "ar-j", "San Juan",
      "ar-k": "ar-k", "Catamarca",
      "ar-l": "ar-l", "La Pampa",
      "ar-m": "ar-m", "Mendoza",
      "ar-n": "ar-n", "Misiones",
      "ar-p": "ar-p", "Formosa",
      "ar-q": "ar-q", "Neuquén",
      "ar-r": "ar-r", "Río Negro",
      "ar-s": "ar-s", "Santa Fé",
      "ar-t": "ar-t", "Tucumán",
      "ar-u": "ar-u", "Chubut",
      "ar-v": "ar-v", "Tierra del Fuego",
      "ar-w": "ar-w", "Corrientes",
      "ar-x": "ar-x", "Córdoba",
      "ar-y": "ar-y", "San Salvador de Jujuy",
      "ar-z": "ar-z", "Santa Cruz",
      "buenos aires"       : "ar-b","Buenos Aires",
      "catamarca"          : "ar-k","Catamarca",
      "chaco"              : "ar-h","Chaco",
      "cordoba"            : "Córdoba",
      "corrientes"         : "Corrientes",
      "la pampa"           : "La Pampa",
      "la rioja"           : "La Rioja",
      "neuquen"            : "Neuquén",
      "rio negro"          : "Río Negro",
      "salta"              : "Salta",
      "san luis"           : "San Luis",
      "santa fe"           : "Santa Fe",
      "santiago del estero": "Santiago del Estero",
      "tucuman"            : "Tucumán",
      "caba"               : "CABA",
      "jujuy"              : "Jujuy",
      "formosa"            : "Formosa",
      "misiones"           : "Misiones",
      "capital federal"    : "CABA",
      "mendoza"            : "Mendoza",
      "error": "Error"
    }

    def __init__(
        self,
        leaf: Leaf,
        reader: SampleReader,
        db_table: dict[str, Any]
    ) -> None:
        super().__init__(leaf, reader, "states", db_table)

    def iterate(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{classname(self)}.iterate: start")
        for row in self.reader.points_of_sale():
            state_code = row.get("state", False)
            state_name = DBStatesGenerator.CODES_AND_STATES.get(state_code, False) if state_code else False
            if state_name:
                test = not self.table.get(state_code, False)
                if test:
                    self.table[state_code] = True
                    yield {"code": state_code, "name": state_name}
            else:
                log_error_row(row)
                message = f"***{classname(self)}.iterate stop***"
                raise DatabaseUpdateException(message)
        logger.info(f"{classname(self)}.iterate: done {len(self.table)}")



class DatabaseInsertOperator:
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
            raise DatabaseInsertException(message)
        try:
            query = select(model_class).where(model_attribute == value)
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"{self.__class__.__name__}._get_unique_record: An error occurred while querying for {row_key} '{value}': {panic}"
            raise DatabaseInsertException(message) from panic

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
            DatabaseInsertException: If an error occurs during the database query.
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
            raise DatabaseInsertException(message) from panic

    def get_timestamp(self, row: dict[str, Any]) -> Timestamp | None:
        """
        Queries for a Timestamp record by its components (year, month, day, etc.).
        This method is custom due to the composite nature of the query.
        """
        timestamp_string = row.get("timestamp")
        if not timestamp_string:
            message = f"{self.__class__.__name__}.get_timestamp: No 'timestamp' found in row: {row_to_string(row)}"
            raise DatabaseInsertException(message)
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
            raise DatabaseInsertException(message) from panic

    def get_city(self, row: dict[str, Any]) -> City | None:
        """
        Queries the database for a City record by its name and the code of its state.
        This composite lookup is necessary because city names are not unique across
        different states.

        Args:
            row: A dict[str, Any] with 'city' and 'state_code' keys.

        Returns:
            The City object if found, otherwise None.
            Raises an exception if multiple cities are found with the same name
            and state code, which would indicate a data integrity issue.
        """
        city_name = row.get("city")
        state_code = row.get("state")

        if not city_name or not state_code:
            message = f"{self.__class__.__name__}.get_city: Missing 'city' or 'state_code' in row: {row_to_string(row)}"
            raise DatabaseInsertException(message)

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
            raise DatabaseInsertException(message) from panic

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
        return self._get_unique_record(PointOfSale, PointOfSale.code, row, "code_and_flag")

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
            raise DatabaseInsertException(message) from panic

    # --- Data Preparation Methods (`*_from_row`) ---

    def timestamp_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        timestamp_string = row.get("timestamp")
        if timestamp_string is None:
            raise DatabaseInsertException(f"Missing 'timestamp' in row: {row_to_string(row)}")
        return timestamp_string_to_row(timestamp_string)

    def state_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        code = row.get("code")
        name = row.get("name")
        if not code or not name:
            raise DatabaseInsertException(f"Missing 'code' or 'name' for state in row: {row_to_string(row)}")
        return {"code": code, "name": name}

    def city_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        state_code = row.get("state")
        if state_code is None:
            raise DatabaseInsertException(f"Missing state in row: {row_to_string(row)}")
        state = self.get_state({"code": state_code})
        if state is None:
            raise DatabaseInsertException(f"Missing state in database: {row_to_string(row)}")
        name = row.get("city")
        if name is None:
            raise DatabaseInsertException(f"Missing 'city' name in row: {row_to_string(row)}")
        return {"name": name, "state_id": state.id}

    def place_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        city = self.get_city(row)
        address = row.get("address")
        if not city or not address:
            raise DatabaseInsertException(f"Missing city or 'address' in row: {row_to_string(row)}")
        return {"address": address, "city_id": city.id}

    def flag_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        name = row.get("flag")
        if name is None:
            raise DatabaseInsertException(f"Missing 'flag' in row: {row_to_string(row)}")
        return {"name": name}

    def business_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        name = row.get("business")
        if name is None:
            raise DatabaseInsertException(f"Missing 'business' in row: {row_to_string(row)}")
        return {"name": name}

    def branch_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        name = row.get("branch")
        if name is None:
            raise DatabaseInsertException(f"Missing 'branch' in row: {row_to_string(row)}")
        return {"name": name}

    def article_code_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        code = row.get("article_code")
        if code is None:
            raise DatabaseInsertException(f"Missing 'article_code' in row: {row_to_string(row)}")
        return {"code": code}

    def article_brand_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        brand = row.get("brand")
        if brand is None:
            raise DatabaseInsertException(f"Missing 'brand' in row: {row_to_string(row)}")
        return {"brand": brand}

    def article_description_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        description = row.get("description")
        if description is None:
            raise DatabaseInsertException(f"Missing 'description' in row: {row_to_string(row)}")
        return {"description": description}

    def article_package_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        package = row.get("package")
        if package is None:
            raise DatabaseInsertException(f"Missing 'package' in row: {row_to_string(row)}")
        return {"package": package}

    def article_card_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        brand = self.get_article_brand(row)
        description = self.get_article_description(row)
        package = self.get_article_package(row)
        code = self.get_article_code(row)
        if not all([brand, description, package, code]):
            raise DatabaseInsertException(f"Missing components for article card in row: {row_to_string(row)}")
        return {
            "brand_id": brand.id,
            "description_id": description.id,
            "package_id": package.id,
            "code_id": code.id,
        }

    def price_from_row(
        self,
        row: dict[str, Any],
        timestamp: Timestamp
    ) -> dict[str, Any]:
        amount = get_int(row.get("price"))
        article_code = self.get_article_code(row)
        point_of_sale = self.get_point_of_sale(row)
        if not all([amount, article_code, point_of_sale]):
            raise DatabaseInsertException(f"Missing components for price in row: {row_to_string(row)}")
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
            raise DatabaseInsertException(message) from panic

    def insert_timestamps(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "timestamps"

        attribute = "timestamp"
        timestamps_list = self._get_all_records(Timestamp)
        all_the_timestamps = SearchTable(attribute, {a_timestamp.search_key(): a_timestamp for a_timestamp in timestamps_list})

        bulk = [
            self.timestamp_from_row(row)
            for row in generate_rows()
            if all_the_timestamps.search(row) is None
        ]
        self._execute_bulk_insert(Timestamp, bulk, context_name)

    def insert_states(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:

        context_name = "states"

        def get_key(row: dict[str, Any]) -> str:
            return row["state"]

        states_list = self._get_all_records(State)
        all_the_states = SearchTable(get_key, {a_state.search_key(): a_state for a_state in states_list})

        bulk = [
            self.state_from_row(row)
            for row in generate_rows()
            if all_the_states.search(row) is None
        ]
        self._execute_bulk_insert(State, bulk, context_name)

    def insert_cities(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "cities"

        attribute = "city_key"
        cities_list = self._get_all_records(City)
        all_the_cities = SearchTable(attribute, {a_city.search_key(): a_city for a_city in cities_list})

        bulk = [
            self.city_from_row(row, all_the_cities)
            for row in generate_rows()
            if all_the_cities.search(row) is None
        ]
        self._execute_bulk_insert(City, bulk, context_name)

    def insert_places(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "places"

        attribute = "place_key"
        places_list = self._get_all_records(Place)
        all_the_places = SearchTable(attribute, {a_place.search_key(): a_place for a_place in places_list})

        bulk = [
            self.place_from_row(row, all_the_places)
            for row in generate_rows()
            if all_the_places.search(row) is None
        ]
        self._execute_bulk_insert(Place, bulk, context_name)

    def insert_flags(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "flags"

        attribute = "flag"
        flags_list = self._get_all_records(Flag)
        all_the_flags = SearchTable(attribute, {a_flag.search_key(): a_flag for a_flag in flags_list})

        bulk = [
            self.flag_from_row(row, all_the_flags)
            for row in generate_rows()
            if all_the_flags.search(row) is None
        ]
        self._execute_bulk_insert(Flag, bulk, context_name)

    def insert_businesses(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "businesses"

        attribute = "business"
        businesses_list = self._get_all_records(Business)
        all_the_businesses = SearchTable(attribute, {a_business.search_key(): a_business for a_business in businesses_list})

        bulk = [
            self.business_from_row(row, all_the_businesses)
            for row in generate_rows()
            if all_the_businesses.search(row) is None
        ]
        self._execute_bulk_insert(Business, bulk, context_name)

    def insert_branches(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "branches"

        attribute = "branch"
        branches_list = self._get_all_records(Branch)
        all_the_branches = SearchTable(attribute, {a_branch.search_key(): a_branch for a_branch in branches_list})

        bulk = [
            self.branch_from_row(row, all_the_branches)
            for row in generate_rows()
            if all_the_branches.search(row) is None
        ]
        self._execute_bulk_insert(Branch, bulk, context_name)

    def insert_article_codes(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "article_codes"

        attribute = "article_code"
        article_codes_list = self._get_all_records(ArticleCode)
        all_the_article_codes = SearchTable(
            search,
            {an_article_code.search_key(): an_article_code for an_article_code in article_codes_list}
        )

        bulk = [
            self.article_code_from_row(row, all_the_article_codes)
            for row in generate_rows()
            if all_the_article_codes.search(row) is None
        ]
        self._execute_bulk_insert(ArticleCode, bulk, context_name)

    def insert_article_brands(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "article_brands"

        attribute = "brand"
        article_brands_list = self._get_all_records(ArticleBrand)
        all_the_article_brands = SearchTable(
            search,
            {an_article_brand.search_key(): an_article_brand for an_article_brand in article_brands_list}
        )

        bulk = [
            self.article_brand_from_row(row, all_the_article_brands)
            for row in generate_rows()
            if all_the_article_brands.search(row) is None
        ]
        self._execute_bulk_insert(ArticleBrand, bulk, context_name)

    def insert_article_descriptions(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        context_name = "article_descriptions"

        attribute = "description"
        article_descriptions_list = self._get_all_records(ArticleDescription)
        all_the_article_descriptions = SearchTable(
            search,
            {an_article_description.search_key(): an_article_description for an_article_description in article_descriptions_list}
        )

        bulk = [
            self.article_description_from_row(row, all_the_article_descriptions)
            for row in generate_rows()
            if all_the_article_descriptions.search(row) is None
        ]
        self._execute_bulk_insert(ArticleDescription, bulk, context_name)

    def insert_article_packages(
        self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        context_name = "article_packages"

        attribute = "package"
        article_packages_list = self._get_all_records(ArticlePackage)
        all_the_article_packages = SearchTable(
            context_name,
            {an_article_package.search_key(): an_article_package for an_article_package in article_packages_list}
        )

        bulk = [
            self.article_package_from_row(row, all_the_article_packages)
            for row in generate_rows()
            if self.get_article_package(row) is None
        ]
        self._execute_bulk_insert(ArticlePackage, bulk, context_name)

    def insert_article_cards(self, generate_rows: Callable[[], Generator[dict[str, Any], None, None]]) -> None:
        context_name = "article_cards"

        attribute = "article_card_key"
        article_cards_list = self._get_all_records(ArticleCard)
        all_the_article_cards = SearchTable(
            context_name,
            {an_article_card.search_key(): an_article_card for an_article_card in article_cards_list}
        )

        bulk = [
            self.article_card_from_row(row, all_the_article_cards)
            for row in generate_rows()
            if all_the_article_card.search(row) is None]
        self._execute_bulk_insert(ArticleCard, bulk, context_name)

    def insert_prices(
        self,
        generate_rows: Callable[[], Generator[dict[str, Any], None, None]],
        timestamp: Timestamp,
    ) -> None:
        # Note: Prices are always inserted as new records for a given timestamp
        # and are not checked for pre-existence, which is the intended behavior.
        context_name = "prices"
        bulk = [self.price_from_row(row, timestamp) for row in generate_rows()]
        self._execute_bulk_insert(Price, bulk, context_name)


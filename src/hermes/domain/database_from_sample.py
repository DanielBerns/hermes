import logging
from collections.abc import Callable
from typing import Any, Generator, Type, List, Dict

from sqlalchemy import insert, select, and_
from sqlalchemy.orm import Session

from hermes.core.search_table import SearchTable
from hermes.core.tree_store import Store
from hermes.domain.rows_ops import row_to_string, timestamp_string_to_row
from hermes.domain.sample_reader import SampleReader
from hermes.domain.models import (
    ArticleBrand, ArticleCard, ArticleCode, ArticleDescription, ArticlePackage,
    Base, Branch, Business, City, Flag, Place, PointOfSale, Price, State, Timestamp
)

logger = logging.getLogger(__name__)


class DatabaseFromSampleException(Exception):
    """Custom exception for database-related errors."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


CODES_AND_STATES = {
  "ar-a": ("ar-a", "Salta"), "salta": ("ar-a", "Salta"),
  "ar-b": ("ar-b", "Buenos Aires"), "buenos aires": ("ar-b", "Buenos Aires"),
  "ar-c": ("ar-c", "CABA"), "caba": ("ar-c", "CABA"), "capital federal": ("ar-c", "CABA"),
  "ar-d": ("ar-d", "San Luis"), "san luis": ("ar-d", "San Luis"),
  "ar-e": ("ar-e", "Entre Rios"),
  "ar-f": ("ar-f", "La Rioja"), "la rioja": ("ar-f", "La Rioja"),
  "ar-g": ("ar-g", "Santiago del Estero"), "santiago del estero": ("ar-g", "Santiago del Estero"),
  "ar-h": ("ar-h", "Chaco"), "chaco": ("ar-h", "Chaco"),
  "ar-j": ("ar-j", "San Juan"),
  "ar-k": ("ar-k", "Catamarca"), "catamarca": ("ar-k", "Catamarca"),
  "ar-l": ("ar-l", "La Pampa"), "la pampa": ("ar-l", "La Pampa"),
  "ar-m": ("ar-m", "Mendoza"), "mendoza": ("ar-m", "Mendoza"),
  "ar-n": ("ar-n", "Misiones"), "misiones": ("ar-n", "Misiones"),
  "ar-p": ("ar-p", "Formosa"), "formosa": ("ar-p", "Formosa"),
  "ar-q": ("ar-q", "Neuquén"), "neuquen": ("ar-q", "Neuquén"),
  "ar-r": ("ar-r", "Río Negro"), "rio negro": ("ar-r", "Río Negro"),
  "ar-s": ("ar-s", "Santa Fé"), "santa fe": ("ar-s", "Santa Fe"),
  "ar-t": ("ar-t", "Tucumán"), "tucuman": ("ar-t", "Tucumán"),
  "ar-u": ("ar-u", "Chubut"),
  "ar-v": ("ar-v", "Tierra del Fuego"),
  "ar-w": ("ar-w", "Corrientes"),
  "ar-x": ("ar-x", "Córdoba"), "cordoba": ("ar-x", "Córdoba"),
  "ar-y": ("ar-y", "Jujuy"), "jujuy": ("ar-y", "Jujuy"),
  "ar-z": ("ar-z", "Santa Cruz"),
  "error": ("xxxx", "Error")
}


class SampleTransformer:
    """Transforms raw sample rows into structured dictionaries for the database."""

    def __init__(self, db_interface: 'DatabaseFromSample'):
        self._db = db_interface

    def timestamp_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        timestamp_string = row.get("timestamp")
        if timestamp_string is None:
            raise DatabaseFromSampleException(f"Missing 'timestamp' in row: {row_to_string(row)}")
        return timestamp_string_to_row(timestamp_string)

    def state_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        state = row.get("state")
        if not state:
            raise DatabaseFromSampleException(f"Missing 'state' in row: {row_to_string(row)}")
        code, name = CODES_AND_STATES.get(state, ("xxxx", "Error"))
        return {"code": code, "name": name}

    def city_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        state_row = self.state_from_row(row)
        state_obj = self._db.get_state(state_row)
        if state_obj is None:
            raise DatabaseFromSampleException(f"State not found in DB for row: {row_to_string(row)}")
        city = row.get("city")
        if city is None:
            raise DatabaseFromSampleException(f"Missing 'city' in row: {row_to_string(row)}")
        return {"name": city, "state_id": state_obj.id}

    def place_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        city_row = self.city_from_row(row)
        city_obj = self._db.get_city(city_row)
        if not city_obj:
            raise DatabaseFromSampleException(f"City not found in DB for row: {row_to_string(row)}")
        address = row.get("address")
        if address is None:
            raise DatabaseFromSampleException(f"Missing 'address' in row: {row_to_string(row)}")
        return {"address": address, "city_id": city_obj.id}

    def flag_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        flag = row.get("flag")
        if flag is None:
            raise DatabaseFromSampleException(f"Missing 'flag' in row: {row_to_string(row)}")
        return {"flag": flag}

    def business_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        business = row.get("business")
        if business is None:
            raise DatabaseFromSampleException(f"Missing 'business' in row: {row_to_string(row)}")
        return {"business": business}

    def branch_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        branch = row.get("branch")
        if branch is None:
            raise DatabaseFromSampleException(f"Missing 'branch' in row: {row_to_string(row)}")
        return {"branch": branch}

    def point_of_sale_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        point_of_sale_key = row.get("point_of_sale_key")
        if point_of_sale_key is None:
            raise DatabaseFromSampleException(f"Missing 'point_of_sale_key' in row: {row_to_string(row)}")

        flag = self._db.get_flag(self.flag_from_row(row))
        business = self._db.get_business(self.business_from_row(row))
        branch = self._db.get_branch(self.branch_from_row(row))

        if not all([flag, business, branch]):
            raise DatabaseFromSampleException(f"Missing flag, business, or branch in DB for row: {row_to_string(row)}")

        return {"code": point_of_sale_key, "flag_id": flag.id, "business_id": business.id, "branch_id": branch.id}

    def point_of_sale_at_place_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        place = self._db.get_place(self.place_from_row(row))
        point_of_sale = self._db.get_point_of_sale(self.point_of_sale_from_row(row))

        if not place or not point_of_sale:
            raise DatabaseFromSampleException(f"Missing place or point_of_sale in DB for row: {row_to_string(row)}")

        return {"place": place, "point_of_sale": point_of_sale}

    def article_code_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        code = row.get("article_code")
        if code is None:
            raise DatabaseFromSampleException(f"Missing 'article_code' in row: {row_to_string(row)}")
        return {"code": code}

    def article_brand_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        brand = row.get("brand")
        if brand is None:
            raise DatabaseFromSampleException(f"Missing 'brand' in row: {row_to_string(row)}")
        return {"brand": brand}

    def article_description_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        description = row.get("description")
        if description is None:
            raise DatabaseFromSampleException(f"Missing 'description' in row: {row_to_string(row)}")
        return {"description": description}

    def article_package_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        package = row.get("package")
        if package is None:
            raise DatabaseFromSampleException(f"Missing 'package' in row: {row_to_string(row)}")
        return {"package": package}

    def article_card_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        brand = self._db.get_article_brand(self.article_brand_from_row(row))
        description = self._db.get_article_description(self.article_description_from_row(row))
        package = self._db.get_article_package(self.article_package_from_row(row))
        article_code = self._db.get_article_code(self.article_code_from_row(row))

        if not all([brand, description, package, article_code]):
            raise DatabaseFromSampleException(f"Missing article component in DB for row: {row_to_string(row)}")

        return {"brand_id": brand.id, "description_id": description.id, "package_id": package.id, "code_id": article_code.id}

    def price_from_row(self, row: dict[str, Any], timestamp: Timestamp) -> dict[str, Any]:
        try:
            amount = int(row["price"])
        except (ValueError, TypeError, KeyError) as e:
            raise DatabaseFromSampleException(f"Invalid or missing 'price' in row {row_to_string(row)}: {e}") from e

        article_code = self._db.get_article_code(self.article_code_from_row(row))
        point_of_sale = self._db.get_point_of_sale({"code": row.get("point_of_sale_key")})

        if not article_code or not point_of_sale:
            raise DatabaseFromSampleException(f"Missing article_code or point_of_sale in DB for price row: {row_to_string(row)}")

        return {"amount": amount, "timestamp_id": timestamp.id, "article_code_id": article_code.id, "point_of_sale_id": point_of_sale.id}


class DatabaseFromSample:
    """Orchestrates reading data from a sample and inserting it into the database."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._transformer = SampleTransformer(self)

    @property
    def session(self) -> Session:
        return self._session

    # GETTER METHODS
    def _get_unique_record(self, model_class: Type[Base], filters: Dict[str, Any]) -> Base | None:
        try:
            query = select(model_class).filter_by(**filters)
            return self.session.execute(query).scalar_one_or_none()
        except Exception as panic:
            message = f"Error querying {model_class.__name__} with {filters}: {panic}"
            raise DatabaseFromSampleException(message) from panic

    def get_timestamp(self, row: dict[str, Any]) -> Timestamp | None:
        return self._get_unique_record(Timestamp, row)

    def get_state(self, row: dict[str, Any]) -> State | None:
        return self._get_unique_record(State, {"code": row.get("code")})

    def get_city(self, row: dict[str, Any]) -> City | None:
        return self._get_unique_record(City, {"name": row.get("name"), "state_id": row.get("state_id")})

    def get_place(self, row: dict[str, Any]) -> Place | None:
        return self._get_unique_record(Place, {"address": row.get("address"), "city_id": row.get("city_id")})

    def get_flag(self, row: dict[str, Any]) -> Flag | None:
        return self._get_unique_record(Flag, {"flag": row.get("flag")})

    def get_business(self, row: dict[str, Any]) -> Business | None:
        return self._get_unique_record(Business, {"business": row.get("business")})

    def get_branch(self, row: dict[str, Any]) -> Branch | None:
        return self._get_unique_record(Branch, {"branch": row.get("branch")})

    def get_point_of_sale(self, row: dict[str, Any]) -> PointOfSale | None:
        return self._get_unique_record(PointOfSale, {"code": row.get("code")})

    def get_article_code(self, row: dict[str, Any]) -> ArticleCode | None:
        return self._get_unique_record(ArticleCode, {"code": row.get("code")})

    def get_article_brand(self, row: dict[str, Any]) -> ArticleBrand | None:
        return self._get_unique_record(ArticleBrand, {"brand": row.get("brand")})

    def get_article_description(self, row: dict[str, Any]) -> ArticleDescription | None:
        return self._get_unique_record(ArticleDescription, {"description": row.get("description")})

    def get_article_package(self, row: dict[str, Any]) -> ArticlePackage | None:
        return self._get_unique_record(ArticlePackage, {"package": row.get("package")})

    def get_article_card(self, row: dict[str, Any]) -> ArticleCard | None:
        return self._get_unique_record(ArticleCard, row)


    # BULK INSERTION
    def _bulk_insert(self, model_class: Type[Base], bulk_data: List[dict[str, Any]]) -> None:
        if not bulk_data:
            return
        try:
            self.session.execute(insert(model_class), bulk_data)
            self.session.commit()
            logger.info(f"Successfully inserted {len(bulk_data)} new records into '{model_class.__tablename__}'.")
        except Exception as panic:
            self.session.rollback()
            raise DatabaseFromSampleException(f"Bulk insert failed for '{model_class.__tablename__}': {panic}") from panic

    def _generate_unique_rows(
        self,
        sample_rows: List[Dict[str, Any]],
        transformer_func: Callable,
        retriever_func: Callable,
        search_keys: List[str]
    ) -> Generator[Dict[str, Any], None, None]:
        search_table = SearchTable(search_keys)
        for row in sample_rows:
            try:
                candidate = transformer_func(row)
                if search_table.insert(candidate):
                    if not retriever_func(candidate):
                        yield candidate
            except DatabaseFromSampleException as e:
                logger.warning(f"Skipping row due to transformation error: {e}")
                continue


    # PROCESSING LOGIC
    def _process_timestamps(self, reader: SampleReader) -> Timestamp:
        timestamp_row = reader.timestamp_row()
        if self.get_timestamp(self._transformer.timestamp_from_row(timestamp_row)):
            raise DatabaseFromSampleException(f"Timestamp {timestamp_row['timestamp']} already processed.")

        self._bulk_insert(Timestamp, [self._transformer.timestamp_from_row(timestamp_row)])

        current_timestamp = self.get_timestamp(self._transformer.timestamp_from_row(timestamp_row))
        if not current_timestamp:
             raise DatabaseFromSampleException("Failed to retrieve current timestamp after insertion.")
        return current_timestamp

    def _process_locations(self, points_of_sale_rows: List[Dict[str, Any]]):
        # States
        state_rows = list(self._generate_unique_rows(points_of_sale_rows, self._transformer.state_from_row, self.get_state, ["code", "name"]))
        self._bulk_insert(State, state_rows)
        # Cities
        city_rows = list(self._generate_unique_rows(points_of_sale_rows, self._transformer.city_from_row, self.get_city, ["name", "state_id"]))
        self._bulk_insert(City, city_rows)
        # Places
        place_rows = list(self._generate_unique_rows(points_of_sale_rows, self._transformer.place_from_row, self.get_place, ["address", "city_id"]))
        self._bulk_insert(Place, place_rows)

    def _process_points_of_sale(self, points_of_sale_rows: List[Dict[str, Any]]):
        # Flags, Businesses, Branches
        self._bulk_insert(Flag, list(self._generate_unique_rows(points_of_sale_rows, self._transformer.flag_from_row, self.get_flag, ["flag"])))
        self._bulk_insert(Business, list(self._generate_unique_rows(points_of_sale_rows, self._transformer.business_from_row, self.get_business, ["business"])))
        self._bulk_insert(Branch, list(self._generate_unique_rows(points_of_sale_rows, self._transformer.branch_from_row, self.get_branch, ["branch"])))

        # PointOfSale
        pos_rows = list(self._generate_unique_rows(points_of_sale_rows, self._transformer.point_of_sale_from_row, self.get_point_of_sale, ["code"]))
        self._bulk_insert(PointOfSale, pos_rows)

        # PointOfSale at Place associations
        for row in points_of_sale_rows:
            try:
                assoc = self._transformer.point_of_sale_at_place_from_row(row)
                place, point_of_sale = assoc["place"], assoc["point_of_sale"]
                if place not in point_of_sale.places:
                    point_of_sale.places.append(place)
            except DatabaseFromSampleException as e:
                logger.warning(f"Could not create POS-Place association: {e}")
        self.session.commit()

    def _process_articles(self, articles_rows: List[Dict[str, Any]]):
        self._bulk_insert(ArticleCode, list(self._generate_unique_rows(articles_rows, self._transformer.article_code_from_row, self.get_article_code, ["code"])))
        self._bulk_insert(ArticleBrand, list(self._generate_unique_rows(articles_rows, self._transformer.article_brand_from_row, self.get_article_brand, ["brand"])))
        self._bulk_insert(ArticleDescription, list(self._generate_unique_rows(articles_rows, self._transformer.article_description_from_row, self.get_article_description, ["description"])))
        self._bulk_insert(ArticlePackage, list(self._generate_unique_rows(articles_rows, self._transformer.article_package_from_row, self.get_article_package, ["package"])))

        card_rows = list(self._generate_unique_rows(articles_rows, self._transformer.article_card_from_row, self.get_article_card, ["brand_id", "description_id", "package_id", "code_id"]))
        self._bulk_insert(ArticleCard, card_rows)

    def _process_prices(self, articles_rows: List[Dict[str, Any]], timestamp: Timestamp):
        price_rows = [self._transformer.price_from_row(row, timestamp) for row in articles_rows]
        self._bulk_insert(Price, price_rows)


    def read_and_process_sample(self, store: Store) -> None:
        """Main orchestration method."""
        logger.info(f"Processing sample from store: {store.key}")
        reader = SampleReader(store)

        try:
            current_timestamp = self._process_timestamps(reader)
        except DatabaseFromSampleException as e:
            logger.warning(e) # Already processed
            return

        points_of_sale_rows = list(reader.points_of_sale())
        articles_rows = list(reader.articles_by_point_of_sale())

        self._process_locations(points_of_sale_rows)
        self._process_points_of_sale(points_of_sale_rows)
        self._process_articles(articles_rows)
        self._process_prices(articles_rows, current_timestamp)

        logger.info(f"Finished processing sample from store: {store.key}")

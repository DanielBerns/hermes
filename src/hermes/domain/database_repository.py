import logging
from typing import Any, Callable, Generator, Type, List, Dict

from sqlalchemy import insert
from sqlalchemy.orm import Session

from hermes.core.tree_store import Store
from hermes.domain.sample_reader import SampleReader
from hermes.domain.models import (
    ArticleBrand, ArticleCard, ArticleCode, ArticleDescription, ArticlePackage,
    Base, Branch, Business, City, Flag, Place, PointOfSale, Price, State, Timestamp
)

logger = logging.getLogger(__name__)

class DatabaseRepositoryException(Exception):
    """Custom exception for repository-related errors."""
    pass

# This data is tightly coupled with how states are processed and stored.
# It belongs with the repository logic that handles states.
CODES_AND_STATES: Dict[str, tuple[str, str]] = {
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

class DatabaseRepository:
    """
    A single, consolidated repository for all database operations,
    acting as the sole interface to the data persistence layer.
    """

    def __init__(self, session: Session):
        self._session = session
        self._cache: Dict[str, Dict[Any, Any]] = {}

    def _bulk_insert(self, model_class: Type[Base], data: List[dict[str, Any]]) -> None:
        """Generic bulk insert method."""
        if not data:
            return
        try:
            self._session.execute(insert(model_class), data)
            logger.info(f"Successfully staged {len(data)} new records for '{model_class.__tablename__}'.")
        except Exception as e:
            raise DatabaseRepositoryException(f"Bulk insert failed for '{model_class.__tablename__}': {e}") from e

    def _get_or_create_cache(self, model_class: Type[Base], key_attr: str) -> Dict[Any, Any]:
        """Creates or retrieves a cache of existing DB objects for a given model."""
        cache_key = model_class.__tablename__
        if cache_key not in self._cache:
            records = self._session.query(model_class).all()
            self._cache[cache_key] = {getattr(rec, key_attr): rec for rec in records}
        return self._cache[cache_key]

    def _invalidate_cache(self, model_class: Type[Base]):
        """Removes a model's cache, forcing a refresh on next access."""
        self._cache.pop(model_class.__tablename__, None)

    def process_sample(self, store: Store) -> None:
        """
        Main entry point to read a sample from a store and persist it to the database.
        This process is idempotent; it will not re-process a sample with the same timestamp.
        """
        reader = SampleReader(store)
        timestamp_str = reader.store.timestamp

        ts = Timestamp.from_string(timestamp_str)
        existing_ts = self._session.query(Timestamp).filter_by(timestamp=ts.timestamp).first()

        if existing_ts:
            logger.warning(f"Sample with timestamp {timestamp_str} already processed. Skipping.")
            return

        self._session.add(ts)
        self._session.flush()

        points_of_sale_data = list(reader.points_of_sale())
        articles_data = list(reader.articles_by_point_of_sale())

        self._process_entities(points_of_sale_data, articles_data, ts.id)

        logger.info(f"Successfully processed sample for timestamp {timestamp_str}.")

    def _process_entities(self, pos_data: list, articles_data: list, timestamp_id: int):
        """Orchestrates the insertion of all entities in the correct order."""
        self._insert_states(pos_data)
        self._insert_simple_entities(pos_data, articles_data)
        self._insert_dependent_entities(pos_data, articles_data)
        self._link_pos_to_places(pos_data)
        self._insert_prices(articles_data, timestamp_id)

    def _transform_state_row(self, row: dict) -> dict:
        """Transforms state data from a sample row to a database row."""
        state_key = row["state"]
        code, name = CODES_AND_STATES.get(state_key, ("xxxx", "Error"))
        return {"code": code, "name": name}

    def _insert_states(self, pos_data: list):
        """Extracts, transforms, and inserts unique state records."""
        cache = self._get_or_create_cache(State, "code")
        new_records = []

        unique_raw_states = {self._transform_state_row(row)["code"]: self._transform_state_row(row) for row in pos_data}.values()

        for state_row in unique_raw_states:
            if state_row["code"] not in cache:
                new_records.append(state_row)

        if new_records:
            self._bulk_insert(State, new_records)
            self._invalidate_cache(State)

    def _insert_simple_entities(self, pos_data: list, articles_data: list):
        """Handles insertion for simple entities that have no dependencies."""
        entity_map = [
            (pos_data, Flag, "flag", "flag"),
            (pos_data, Business, "business", "business"),
            (pos_data, Branch, "branch", "branch"),
            (articles_data, ArticleCode, "article_code", "code"),
            (articles_data, ArticleBrand, "brand", "brand"),
            (articles_data, ArticleDescription, "description", "description"),
            (articles_data, ArticlePackage, "package", "package"),
        ]

        for data, model, sample_key, db_key in entity_map:
            self._insert_unique_from_sample(data, model, sample_key, db_key, lambda r: {db_key: r[sample_key]})

    def _insert_unique_from_sample(self, sample_data: list, model: Type[Base], sample_key: str, db_key: str, transform_func: Callable):
        """Generic method to extract, transform, and insert unique entities."""
        cache = self._get_or_create_cache(model, db_key)
        new_records = []

        unique_values = {row[sample_key] for row in sample_data if sample_key in row}

        for value in unique_values:
            # Using the raw value to check against the cache key
            if value not in cache:
                transformed_row = transform_func({sample_key: value})
                new_records.append(transformed_row)

        if new_records:
            self._bulk_insert(model, new_records)
            self._invalidate_cache(model)

    def _insert_dependent_entities(self, pos_data: list, articles_data: list):
        """Handles insertion for entities that depend on others."""
        self._insert_cities(pos_data)
        self._insert_places(pos_data)
        self._insert_points_of_sale(pos_data)
        self._insert_article_cards(articles_data)

    def _insert_cities(self, pos_data: list):
        states_cache = self._get_or_create_cache(State, "code")
        cities_cache = self._get_or_create_cache(City, "name") # Simplified: assumes city names are unique for demo

        new_cities = []
        unique_city_state_pairs = {(row['city'], row['state']) for row in pos_data}

        for city_name, state_key in unique_city_state_pairs:
            state_code, _ = self._transform_state_row({"state": state_key}).values()
            state = states_cache.get(state_code)

            if state and city_name not in cities_cache:
                new_cities.append({"name": city_name, "state_id": state.id})
                cities_cache[city_name] = True # Avoid duplicates in same run

        if new_cities:
            self._bulk_insert(City, new_cities)
            self._invalidate_cache(City)

    def _insert_places(self, pos_data: list):
        cities_cache = self._get_or_create_cache(City, "name")
        places_cache = self._get_or_create_cache(Place, "address")

        new_places = []
        unique_place_city_pairs = {(row['address'], row['city']) for row in pos_data}

        for address, city_name in unique_place_city_pairs:
            city = cities_cache.get(city_name)
            if city and address not in places_cache:
                new_places.append({"address": address, "city_id": city.id})
                places_cache[address] = True

        if new_places:
            self._bulk_insert(Place, new_places)
            self._invalidate_cache(Place)

    def _insert_points_of_sale(self, pos_data: list):
        # Ensure dependent caches are fresh
        flags_cache = self._get_or_create_cache(Flag, "flag")
        businesses_cache = self._get_or_create_cache(Business, "business")
        branches_cache = self._get_or_create_cache(Branch, "branch")
        pos_cache = self._get_or_create_cache(PointOfSale, "code")

        new_pos = []
        for row in pos_data:
            point_of_sale_key = row["point_of_sale_key"]
            if point_of_sale_key not in pos_cache:
                flag = flags_cache.get(row["flag"])
                business = businesses_cache.get(row["business"])
                branch = branches_cache.get(row["branch"])
                if flag and business and branch:
                    new_pos.append({
                        "code": point_of_sale_key,
                        "flag_id": flag.id,
                        "business_id": business.id,
                        "branch_id": branch.id
                    })
                    pos_cache[point_of_sale_key] = True

        if new_pos:
            self._bulk_insert(PointOfSale, new_pos)
            self._invalidate_cache(PointOfSale)

    def _insert_article_cards(self, articles_data: list):
        brands_cache = self._get_or_create_cache(ArticleBrand, "brand")
        descs_cache = self._get_or_create_cache(ArticleDescription, "description")
        packs_cache = self._get_or_create_cache(ArticlePackage, "package")
        codes_cache = self._get_or_create_cache(ArticleCode, "code")

        new_cards = []
        existing_cards = { (c.brand.brand, c.description.description, c.package.package, c.code.code) for c in self._session.query(ArticleCard).all()}

        for row in articles_data:
            brand = brands_cache.get(row["brand"])
            desc = descs_cache.get(row["description"])
            pack = packs_cache.get(row["package"])
            code = codes_cache.get(row["article_code"])

            if brand and desc and pack and code:
                card_tuple = (brand.brand, desc.description, pack.package, code.code)
                if card_tuple not in existing_cards:
                    new_cards.append({
                        "brand_id": brand.id,
                        "description_id": desc.id,
                        "package_id": pack.id,
                        "code_id": code.id,
                    })
                    existing_cards.add(card_tuple)

        if new_cards:
            self._bulk_insert(ArticleCard, new_cards)

    def _link_pos_to_places(self, pos_data: list):
        places_cache = self._get_or_create_cache(Place, "address")
        pos_cache = self._get_or_create_cache(PointOfSale, "code")

        for row in pos_data:
            place = places_cache.get(row["address"])
            pos = pos_cache.get(row["point_of_sale_key"])
            if place and pos and place not in pos.places:
                 pos.places.append(place)

    def _insert_prices(self, articles_data: list, timestamp_id: int):
        codes_cache = self._get_or_create_cache(ArticleCode, "code")
        pos_cache = self._get_or_create_cache(PointOfSale, "code")

        prices = []
        for row in articles_data:
            code = codes_cache.get(row["article_code"])
            pos = pos_cache.get(row["point_of_sale_key"])
            if code and pos:
                prices.append({
                    "amount": int(row["price"]),
                    "timestamp_id": timestamp_id,
                    "article_code_id": code.id,
                    "point_of_sale_id": pos.id,
                })
        if prices:
            self._bulk_insert(Price, prices)


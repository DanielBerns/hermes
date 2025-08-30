import logging
from typing import Any

logger = logging.getLogger(__name__)

class DataProcessorException(Exception):
    """Custom exception for data processing errors."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)

# --- Key Generation Utilities ---

def get_city_key(state: str, city: str) -> str:
    return f"({state})[{city}]"

def get_place_key(state: str, city: str, address: str) -> str:
    return f"({state})[({city})({address})]"

def get_point_of_sale_key(point_of_sale_code: str, flag: str) -> str:
    return f"({point_of_sale_code})({flag})"

def get_article_card_key(article_code: str, brand: str, description: str, package: str) -> str:
    return f"[{article_code}]({brand})({description})({package})"

# --- Data Transformation Utilities ---

def price_to_cents(price: str) -> int:
    """Converts a string price to an integer number of cents."""
    try:
        return int(float(price) * 100)
    except (ValueError, TypeError) as e:
        raise DataProcessorException(f"Could not convert price '{price}' to cents.") from e

# --- Main Processor Class ---

class DataProcessor:
    """
    A stateful processor that transforms raw scraped data into a structured format.
    It maintains a mapping of point-of-sale codes to keys during its lifetime.
    """
    def __init__(self) -> None:
        self._pos_code_to_key_map: dict[str, str] = {}

    def process_point_of_sale(self, raw_pos_row: dict[str, Any]) -> dict[str, Any]:
        """Processes a raw point of sale dictionary and returns a structured one."""
        try:
            point_of_sale_code = str(raw_pos_row["id"]).strip()
            state = str(raw_pos_row["provincia"]).strip().lower()
            city = str(raw_pos_row["localidad"]).strip().lower()
            address = str(raw_pos_row["direccion"]).strip().lower()
            flag = str(raw_pos_row["banderaDescripcion"]).strip().lower()

            point_of_sale_key = get_point_of_sale_key(point_of_sale_code, flag)

            # Store the mapping for use by the article processor
            self._pos_code_to_key_map[point_of_sale_code] = point_of_sale_key

            return {
                "point_of_sale_code": point_of_sale_code,
                "state": state,
                "city": city,
                "address": address,
                "flag": flag,
                "business": str(raw_pos_row["comercioRazonSocial"]).strip().lower(),
                "branch": str(raw_pos_row["sucursalNombre"]).strip().lower(),
                "city_key": get_city_key(state, city),
                "place_key": get_place_key(state, city, address),
                "point_of_sale_key": point_of_sale_key,
            }
        except KeyError as e:
            raise DataProcessorException(f"Missing expected key in point of sale row: {e}")

    def process_article(self, raw_article_row: dict[str, Any]) -> dict[str, Any]:
        """Processes a raw article dictionary and returns a structured one."""
        try:
            point_of_sale_code = str(raw_article_row["point_of_sale_id"]).strip()
            point_of_sale_key = self._pos_code_to_key_map.get(point_of_sale_code)

            if point_of_sale_key is None:
                raise DataProcessorException(f"Unknown point_of_sale_code '{point_of_sale_code}' in article row. Process points of sale first.")

            article_code = str(raw_article_row["id"]).strip()
            brand = str(raw_article_row["marca"]).strip().lower()
            description = str(raw_article_row["nombre"]).strip().lower()
            package = str(raw_article_row["presentacion"]).strip().lower()

            return {
                "article_code": article_code,
                "brand": brand,
                "description": description,
                "package": package,
                "price": price_to_cents(raw_article_row["precio"]),
                "point_of_sale_code": point_of_sale_code,
                "point_of_sale_key": point_of_sale_key,
                "article_card_key": get_article_card_key(article_code, brand, description, package),
            }
        except KeyError as e:
            raise DataProcessorException(f"Missing expected key in article row: {e}")


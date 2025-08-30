import logging
from typing import Any, Dict

# Set up a logger for this module.
logger = logging.getLogger(__name__)

class DataProcessorException(Exception):
    """Custom exception for data processing errors."""
    pass

# --- Utility Functions ---

def get_int(string: str) -> int:
    """
    Safely convert a string to an integer, raising a custom exception on failure.

    Args:
        string: The string to convert.

    Returns:
        The converted integer.

    Raises:
        DataProcessorException: If the conversion to integer fails.
    """
    try:
        return int(string)
    except (ValueError, TypeError) as e:
        message = f"Failed to convert '{string}' to an integer."
        raise DataProcessorException(message) from e

def get_city_key(state: str, city: str) -> str:
    """Generates a unique key for a city within a state."""
    return f"({state})[{city}]"

def get_place_key(state: str, city: str, address: str) -> str:
    """Generates a unique key for a specific address."""
    return f"({state})[({city})({address})]"

def get_point_of_sale_key(point_of_sale_code: str, flag: str) -> str:
    """Generates a unique key for a point of sale."""
    return f"({point_of_sale_code})({flag})"

def get_article_card_key(article_code: str, brand: str, description: str, package: str) -> str:
    """Generates a unique key for an article card."""
    return f"[{article_code}]({brand})({description})({package})"

def price_to_cents(price: str) -> int:
    """Converts a string price to an integer number of cents."""
    try:
        return int(float(price) * 100)
    except (ValueError, TypeError) as e:
        raise DataProcessorException(f"Invalid price format: '{price}'") from e

# --- Main Processor Class ---

class DataProcessor:
    """
    A stateful processor that transforms raw scraped data into a structured format.
    It maintains a map of processed points of sale to correctly associate articles.
    """

    def __init__(self) -> None:
        self._point_of_sale_map: Dict[str, str] = {}

    def process_point_of_sale(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a raw point of sale dictionary."""
        try:
            pos_code = str(row["id"]).strip()
            state = str(row["provincia"]).strip().lower()
            city = str(row["localidad"]).strip().lower()
            address = str(row["direccion"]).strip().lower()
            flag = str(row["banderaDescripcion"]).strip().lower()

            point_of_sale_key = get_point_of_sale_key(pos_code, flag)

            # Store the mapping for later use by article processor
            self._point_of_sale_map[pos_code] = point_of_sale_key

            return {
                "point_of_sale_code": pos_code,
                "state": state,
                "city": city,
                "address": address,
                "flag": flag,
                "business": str(row["comercioRazonSocial"]).strip().lower(),
                "branch": str(row["sucursalNombre"]).strip().lower(),
                "city_key": get_city_key(state, city),
                "place_key": get_place_key(state, city, address),
                "point_of_sale_key": point_of_sale_key,
            }
        except KeyError as e:
            raise DataProcessorException(f"Missing expected key in point of sale data: {e}")

    def process_article(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a raw article dictionary."""
        try:
            pos_code = str(row["point_of_sale_id"]).strip()

            if pos_code not in self._point_of_sale_map:
                raise DataProcessorException(f"Unknown point_of_sale_id '{pos_code}'. Process its point of sale first.")

            article_code = str(row["id"]).strip()
            brand = str(row["marca"]).strip().lower()
            description = str(row["nombre"]).strip().lower()
            package = str(row["presentacion"]).strip().lower()

            return {
                "article_code": article_code,
                "brand": brand,
                "description": description,
                "package": package,
                "price": price_to_cents(row["precio"]),
                "point_of_sale_code": pos_code,
                "point_of_sale_key": self._point_of_sale_map[pos_code],
                "article_card_key": get_article_card_key(article_code, brand, description, package),
            }
        except KeyError as e:
            raise DataProcessorException(f"Missing expected key in article data: {e}")


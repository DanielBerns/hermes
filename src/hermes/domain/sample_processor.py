import logging
from typing import Any, Tuple

from hermes.core.rows_processor import RowsProcessor

# Set up a logger for this module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SampleProcessorException(Exception):
    """Custom exception for rows ops related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)

class SampleProcessor:
    def __init__(self) -> None:
        self._map_of_points_of_sale: dict[str, Any] = {}

    @property
    def map_of_points_of_sale(self) -> dict[str, Any]:
        return self._map_of_points_of_sale

    def point_of_sale_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        point_of_sale_code = (row["id"]).strip()
        state = (row["provincia"]).strip().lower()
        city = (row["localidad"]).strip().lower()
        address = (row["direccion"]).strip().lower()
        flag = (row["banderaDescripcion"]).strip().lower()
        business = (row["comercioRazonSocial"]).strip().lower()
        branch = (row["sucursalNombre"]).strip().lower()
        city_key = get_city_key(state, city)
        place_key = get_place_key(state, city, address)
        point_of_sale_key = get_point_of_sale_key(point_of_sale_code, flag)
        point_of_sale = {
            "point_of_sale_code": point_of_sale_code,
            "state": state,
            "city": city,
            "address": address,
            "flag": flag,
            "business": business,
            "branch": branch,
            "city_key": city_key,
            "place_key": place_key,
            "point_of_sale_key": point_of_sale_key,
        }
        self.map_of_points_of_sale[point_of_sale_code] = point_of_sale_key
        return point_of_sale

    def article_per_point_of_sale_from_row(
        self,
        row: dict[str, Any]
    ) -> dict[str, Any]:
        article_code = (row["id"]).strip()
        brand = (row["marca"]).strip().lower()
        description = (row["nombre"]).strip().lower()
        package = (row["presentacion"]).strip().lower()
        price = price_to_cents(row["precio"])
        point_of_sale_code = (row["point_of_sale_id"]).strip()
        point_of_sale_key = self.map_of_point_of_sale[point_of_sale_code]
        article_card_key = get_article_card_key(
            article_code, brand, description, package
        )
        article_per_point_of_sale = {
            "article_code": article_code,
            "brand": brand,
            "description": description,
            "package": package,
            "price": price,
            "point_of_sale_code": point_of_sale_code,
            "point_of_sale_key": point_of_sale_key,
            "article_card_key": article_card_key,
        }
        return article_per_point_of_sale

def get_city_key(state: str, city: str) -> str:
    return f"({state})[{city}]"

def get_place_key(state: str, city: str, address: str) -> str:
    return f"({state})[({city})({address})]"

def get_point_of_sale_key(point_of_sale_code: str, flag: str) -> str:
    return f"({point_of_sale_code})({flag})"

def get_article_code_key(article_code: str) -> str:
    return article_code

def get_article_card_key(article_code: str, brand: str, description: str, package: str) -> str:
    return f"[{article_code}]({brand})({description})({package})"

def price_to_cents(price: str) -> int:
    try:
        return int(float(price) * 100)
    except Exception as panic:
        message = f"sample_processor.price_to_cents: check {price}"
        raise SampleProcessorException(message)

def cents_to_price(cents: str) -> str:
    try:
        return f"{int(cents) / 100:>12.2f}"
    except Exception as panic:
        message = f"sample_processor.cents_to_price: check {cents}"
        raise SampleProcessorException(message)

def get_int(string: str) -> int:
    """
    Safely convert a string to an integer, raising a custom exception on failure.

    Args:
        string: The string to convert.

    Returns:
        The converted integer.

    Raises:
        SampleProcessorException: If the conversion to integer fails.
    """
    try:
        return int(string)
    except Exception as panic:
        message = f"sample_processor.get_int: int('{string}') fails."
        raise SampleProcessorException(message) from panic


def timestamp_string_to_row(timestamp_string: str) -> dict[str, Any]:
    return {
        "year": get_int(timestamp_string[0:4]),
        "month": get_int(timestamp_string[4:6]),
        "day": get_int(timestamp_string[6:8]),
        "hour": get_int(timestamp_string[8:10]),
        "minute": get_int(timestamp_string[10:12]),
        "second": get_int(timestamp_string[12:14]),
    }

def row_to_string(row: dict[str, Any], separator="|") -> str:
    return "{" + separator.join(f"{key}:{value}" for key, value in row.items()) + "}"


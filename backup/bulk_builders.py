import logging
from typing import Any

from hermes.core.search_table import SearchTable
from hermes.domain.database import (
    Timestamp,
    State,
    City,
    PointOfSale,
    ArticleCode,
    ArticleBrand,
    ArticleDescription,
    ArticlePackage,
)
from hermes.domain.rows_ops import timestamp_string_to_row, row_to_string, get_int

# Set up a logger for this module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class BulkBuildersException(Exception):
    """Custom exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


def timestamp_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    timestamp_string = row.get("timestamp", None)
    if timestamp_string is None:
        raise BulkBuildersException(f"timestamp_bulk_from_row {row_to_string(row)}")
    return timestamp_string_to_row(timestamp_string)


def state_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    code = row.get("code", None)
    name = row.get("name", None) if code else None
    if name is None:
        raise BulkBuildersException(f"state_bulk_from_row: {row_to_string(row)}")
    return {"code": code, "name": name}


def city_bulk_from_row(
    row: dict[str, Any], table: SearchTable[State]
) -> dict[str, Any]:
    state = table.search(row)
    name = row.get("city", None) if state else None
    if name is None:
        raise BulkBuildersException(f"city_bulk_from_row {row_to_string(row)}")
    return {"name": name, "state_id": state.id}


def place_bulk_from_row(
    row: dict[str, Any], table: SearchTable[City]
) -> dict[str, Any]:
    city = table.search(row)
    address = row.get("address", None) if city else None
    if address is None:
        raise BulkBuildersException(f"place_bulk_from_row {row_to_string(row)}")
    return {"address": address, "city_id": city.id}


def flag_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    name = row.get("flag", None)
    if name is None:
        raise BulkBuildersException(f"flag_bulk_from_row {row_to_string(row)}")
    return {"name": name}


def business_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    name = row.get("business", None)
    if name is None:
        raise BulkBuildersException(f"business_bulk_from_row {row_to_string(row)}")
    return {"name": name}


def branch_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    name = row.get("branch", None)
    if name is None:
        raise BulkBuildersException(f"branch_bulk_from_row {row_to_string(row)}")
    return {"name": name}


def article_code_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    code = row.get("article_code", None)
    if code is None:
        raise BulkBuildersException(f"article_code_bulk_from_row {row_to_string(row)}")
    return {"code": code}


def article_brand_from_row(row: dict[str, Any]) -> dict[str, Any]:
    brand = row.get("brand", None)
    if brand is None:
        raise BulkBuildersException(f"article_brand_bulk_from_row {row_to_string(row)}")
    return {"brand": brand}


def article_description_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    description = row.get("description", None)
    if description is None:
        raise BulkBuildersException(
            f"article_description_bulk_from_row {row_to_string(row)}"
        )
    return {"description": description}


def article_package_bulk_from_row(row: dict[str, Any]) -> dict[str, Any]:
    package = row.get("package", None)
    if package is None:
        raise BulkBuildersException(
            f"article_package_bulk_from_row {row_to_string(row)}"
        )
    return {"package": package}


def article_card_bulk_from_row(
    row: dict[str, Any],
    brands_table: SearchTable[ArticleBrand],
    descriptions_table: SearchTable[ArticleDescription],
    packages_table: SearchTable[ArticlePackage],
    article_codes_table: SearchTable[ArticleCode],
) -> dict[str, Any]:
    brand = brands_table.search(row)
    description = descriptions_table.search(row) if brand else None
    package = packages_table.search(row) if description else None
    code = article_codes_table.search(row) if package else None
    if code is None:
        raise BulkBuildersException(f"article_card_bulk_from_row {row_to_string(row)}")
    return {
        "brand_id": brand.id,
        "description_id": description.id,
        "package_id": package.id,
        "code": code.id,
    }


def price_bulk_from_row(
    row: dict[str, Any],
    timestamp: Timestamp,
    article_codes_table: SearchTable[ArticleCode],
    points_of_sale_table: SearchTable[PointOfSale],
) -> dict[str, Any]:
    amount = get_int(row.get("price", 0))
    article_code = article_codes_table.search(row) if amount else None
    point_of_sale = points_of_sale_table.search(row) if article_code else None
    if point_of_sale is None:
        raise BulkBuildersException(f"price_bulk_from_row {row_to_string(row)}")
    return {
        "amount": amount,
        "timestamp_id": timestamp.id,
        "article_code_id": article_code.id,
        "point_of_sale_id": point_of_sale.id,
    }

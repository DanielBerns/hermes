from typing import Any, Tuple

from hermes.core.rows_processor import RowsProcessor


class PointOfSaleRowsProcessor:
    def __init__(
        self,
        map_of_points_of_sale: dict[str, Any]
    ) -> None:
        self._map_of_points_of_sale: dict[str, Any] = map_of_points_of_sale

    @property
    def map_of_point_of_sale(self) -> dict[str, Any]:
        return self._map_of_points_of_sale

    def execute(
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
            "point_of_sale_key": point_of_sale_key
        }
        self.map_of_point_of_sale[point_of_sale_code] = point_of_sale_key
        return point_of_sale


class ArticlesByPointOfSaleRowsProcessor:
    def __init__(
        self,
        map_of_points_of_sale: dict[str, Any]
    ) -> None:
        self._map_of_points_of_sale: dict[str, Any] = map_of_points_of_sale

    @property
    def map_of_point_of_sale(self) -> dict[str, Any]:
        return self._map_of_points_of_sale

    def execute(self, row: dict[str, Any]) -> dict[str, Any]:
        article_code = (row["id"]).strip()
        brand = (row["marca"]).strip().lower()
        description = (row["nombre"]).strip().lower()
        package = (row["presentacion"]).strip().lower()
        price = price_to_cents(row["precio"])
        point_of_sale_code = (row["point_of_sale_id"]).strip()
        point_of_sale_key = self.map_of_point_of_sale[point_of_sale_code]
        article_card_key = get_article_card_key(article_code, brand, description, package)
        article_per_point_of_sale = {
            "article_code": article_code,
            "brand": brand,
            "description": description,
            "package": package,
            "price": price,
            "point_of_sale_code": point_of_sale_code,
            "point_of_sale_key": point_of_sale_key,
            "article_card_key": article_card_key
        }
        return article_per_point_of_sale


def get_city_key(state_code: str, city: str) -> str:
    return f"({state_code})[{city}]"

def get_place_key(state_code: str, city: str, address: str) -> str:
    return f"({state_code})[({city})({address})]"

def get_point_of_sale_key(point_of_sale_code: str, flag: str) -> str:
    return f"({point_of_sale_code})({flag})"

def get_article_code_key(article_code: str) -> str:
    return article_code

def get_article_card_key(article_code: str, brand: str, description: str, package: str) -> str:
    return f"[{article_code}]({brand})({description})({package})"

def get_mecon_rows_processors() -> Tuple[RowsProcessor, RowsProcessor]:
    map_of_point_of_sale: dict[str, Any] = {}
    points_of_sale_rows_processor = PointOfSaleRowsProcessor(map_of_point_of_sale)
    articles_by_point_of_sale_rows_processor = ArticlesByPointOfSaleRowsProcessor(map_of_point_of_sale)
    return points_of_sale_rows_processor, articles_by_point_of_sale_rows_processor

def price_to_cents(price: str) -> int:
    return int(float(price) * 100)

def cents_to_price(cents: str) -> str:
    return f"{int(cents)/100:>12.2f}"

def row_to_string(row: dict[str, Any], separator="|") -> str:
    return "{" + separator.join(f"{key}:{value}" for key, value in row.items()) + "}"

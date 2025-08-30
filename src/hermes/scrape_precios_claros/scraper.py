import logging
from dataclasses import dataclass, field
from typing import Any, Generator, Optional, Dict

from hermes.scrape_precios_claros.web_client import WebClient

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Custom exception for errors during the scraping process."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class ScraperStop(BaseException):
    """Custom exception for 'tolerable' errors that should stop the current scrape."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.warning(f"ScraperStop: {message}")


@dataclass
class QueryLimits:
    """Represents API pagination limits."""
    total_items: int
    max_queries_per_page: int
    full_pages: int = field(init=False)
    last_page_queries: int = field(init=False)

    def __post_init__(self) -> None:
        if self.total_items > 0 and self.max_queries_per_page > 0:
            self.full_pages = self.total_items // self.max_queries_per_page
            self.last_page_queries = self.total_items % self.max_queries_per_page
        else:
            self.full_pages = self.last_page_queries = 0
        logger.info(f"QueryLimits initialized: {self}")

    def offset(self, page: int) -> int:
        return page * self.max_queries_per_page


class Scraper:
    """A web scraper for the Precios Claros API."""

    def __init__(self, webclient: WebClient, base_url: str = "https://d3e6htiiul5ek9.cloudfront.net/dev/") -> None:
        self._webclient = webclient
        self._base_url = base_url
        self._endpoints = {
            "points_of_sale": "sucursales",
            "articles": "productos"
        }

    def _get_query_limits(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> QueryLimits:
        """Fetches the total number of items for a given endpoint query."""
        url = f"{self._base_url}{endpoint}"
        query_params = (params or {}).copy()
        query_params['limit'] = 0

        logger.info(f"Fetching query limits from: {url} with params {query_params}")
        try:
            response = self._webclient.get(url, params=query_params)
            total = response.get("total", 0)
            max_limit = response.get("maxLimitPermitido", 0)
            if response.get("errorMessage") or total == 0 or max_limit == 0:
                logger.warning(f"API returned no items or an error for {url}: {response.get('errorMessage', 'No items found')}")
                return QueryLimits(0, 0)
            return QueryLimits(total, max_limit)
        except Exception as e:
            raise ScraperError(f"Error fetching query limits from {url}: {e}") from e

    def _fetch_paginated_data(
        self, endpoint_key: str, result_key: str, params: Optional[Dict[str, Any]] = None, **extra_fields: Any
    ) -> Generator[Dict[str, Any], None, None]:
        """A generic method to fetch data from a paginated API endpoint."""
        endpoint = self._endpoints.get(endpoint_key)
        if not endpoint:
            raise ScraperError(f"Invalid endpoint key: {endpoint_key}")

        query_limits = self._get_query_limits(endpoint, params)
        if query_limits.total_items == 0:
            return

        # Fetch full pages
        for page in range(query_limits.full_pages):
            offset = query_limits.offset(page)
            url = f"{self._base_url}{endpoint}"
            page_params = (params or {}).copy()
            page_params.update({"offset": offset, "limit": query_limits.max_queries_per_page})

            response = self._webclient.get(url, params=page_params)
            items = response.get(result_key)
            if not items:
                raise ScraperStop(f"No '{result_key}' in response from {url} with params {page_params}")

            for item in items:
                item.update(extra_fields)
                yield item

        # Fetch the last page
        if query_limits.last_page_queries > 0:
            offset = query_limits.offset(query_limits.full_pages)
            url = f"{self._base_url}{endpoint}"
            page_params = (params or {}).copy()
            page_params.update({"offset": offset, "limit": query_limits.last_page_queries})

            response = self._webclient.get(url, params=page_params)
            items = response.get(result_key)
            if not items:
                raise ScraperStop(f"No '{result_key}' on last page from {url} with params {page_params}")

            for item in items:
                item.update(extra_fields)
                yield item

    def points_of_sale(self) -> Generator[Dict[str, Any], None, None]:
        """Fetches all points of sale (supermarkets) from the API."""
        logger.info("Starting to fetch all points of sale.")
        yield from self._fetch_paginated_data("points_of_sale", "sucursales")
        logger.info("Finished fetching points of sale.")

    def articles_by_point_of_sale(self, point_of_sale_id: str) -> Generator[Dict[str, Any], None, None]:
        """Fetches all articles for a specific point of sale."""
        logger.info(f"Starting to fetch articles for point_of_sale_id: {point_of_sale_id}")
        params = {"id_sucursal": point_of_sale_id}
        yield from self._fetch_paginated_data(
            "articles", "productos", params=params, point_of_sale_id=point_of_sale_id
        )
        logger.info(f"Finished fetching articles for point_of_sale_id: {point_of_sale_id}")

import logging
from dataclasses import dataclass, field
from typing import Any, Generator, Optional
from hermes.scrape_precios_claros.web_client import WebClient

# Get a named logger for this module
logger = logging.getLogger(__name__)

class ScraperError(Exception):
    """Custom exception for errors during the scraping process."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)

# TODO: Retry attempts where this error appears
class ScraperStop(BaseException):
    """Custom exception for 'tolerable' errors"""
    def __init__(self) -> None:
        super().__init__()
        logger.warning("ScraperStop")


@dataclass
class QueryLimits:
    """
    Represents the total number of items and the maximum items per query page
    allowed by the API.

    Calculates the number of full pages, queries per full page, and queries
    on the last page.
    """
    total_items: int
    max_queries_per_page: int
    full_pages: int = field(init=False)
    full_page_queries: int = field(init=False)
    last_page_queries: int = field(init=False)

    def __post_init__(self) -> None:
        """Calculates pagination details after initialization."""
        if self.total_items > 0 and self.max_queries_per_page > 0:
            self.full_pages = self.total_items // self.max_queries_per_page
            self.full_page_queries = self.max_queries_per_page
            self.last_page_queries = self.total_items % self.max_queries_per_page
        else:
            self.full_pages = self.full_page_queries = self.last_page_queries = 0
        logger.info(
            f"{self.__class__.__name__} initialization\n"
            f"    total_items={self.total_items}\n"
            f"    max_queries_per_page={self.max_queries_per_page}\n"
            f"    full_pages={self.full_pages}\n"
            f"    full_page_queries={self.full_page_queries}\n"
            f"    last_page_queries={self.last_page_queries}\n"
        )


    def offset(self, page: int) -> int:
        """
        Calculates the offset for a given page number.

        Args:
            page: The page number (0-indexed).

        Returns:
            The starting offset for the items on that page.
        """
        return page * self.full_page_queries


class Scraper:
    """
    A web scraper for the Precios Claros API to fetch data about
    points of sale (supermarkets) and articles (products) sold at each point of sale.
    """
    def __init__(self, webclient: WebClient) -> None:
        """
        Initializes the PreciosClarosScraper.

        Args:
            webclient: An instance of a WebClient to make HTTP requests.
            base_url: The base URL for the Precios Claros API.
        """
        self._webclient = webclient
        self._base_url: str = "https://d3e6htiiul5ek9.cloudfront.net/dev/"
        self._points_of_sale_endpoint: str = "sucursales"
        self._articles_endpoint: str = "productos"

    @property
    def webclient(self) -> WebClient:
        """Returns the underlying WebClient instance."""
        return self._webclient

    @property
    def base_url(self) -> str:
        """Returns the base URL of the API."""
        return self._base_url

    def _build_url(self, endpoint: str, offset: int, limit: int, params: Optional[dict[str, Any]] = None) -> str:
        """
        Builds a full API URL with pagination parameters and optional additional parameters.

        Args:
            endpoint: The API endpoint (e.g., "sucursales", "productos").
            offset: The offset for pagination.
            limit: The maximum number of items to return in this query.
            params: Optional dictionary of additional query parameters.

        Returns:
            The constructed URL string.
        """
        url = f"{self.base_url}{endpoint}?offset={offset:d}&limit={limit:d}"
        if params:
            # Simple parameter appending; considers improving for complex cases
            for key, value in params.items():
                 url += f"&{key}={value}"
        return url

    def _get_query_limits(self, url: str) -> QueryLimits:
        """
        Fetches the total number of items and max items per page for a given endpoint query.

        Args:
            url: The initial URL to query with limit=0 to get total and limit info.

        Returns:
            A QueryLimits dataclass instance.

        Raises:
            ScraperError: If the initial query fails.
        """
        logger.info(f"Fetching query limits from: {url}")
        try:
            response = self.webclient.get(url)
            error_message = response.get("errorMessage", None)
            if error_message:
                message = f"{self.__class__.__name__}._get_query_limits: API returned error message for {url}: {error_message}"
                logger.warning(message)
                # Return zero limits instead of raising error immediately
                total_items = max_queries_per_page = 0
            else:
                total_items = response.get("total", 0)
                max_queries_per_page = response.get("maxLimitPermitido", 0)
                if total_items == 0 or max_queries_per_page == 0:
                    logger.warning(f"{self.__class__.__name__}._get_query_limits: Zero total items or max limit for {url}")
                    # Return zero limits instead of raising error immediately
                    total_items = max_queries_per_page = 0
            logger.info(
                f"{self.__class__.__name__}._get_query_limits: total_items={total_items}, max_queries_per_page={max_queries_per_page}"
            )
            return QueryLimits(total_items, max_queries_per_page)
        except Exception as e:
            message = f"{self.__class__.__name__}._get_query_limits: Error fetching query limits from {url}, {e}"
            logger.error(message)
            raise ScraperError(message)

    def _points_of_sale(self, url: str) -> Generator[dict[str, Any], None, None]:
        response = self.webclient.get(url)
        _points_of_sale = response.get("sucursales", None)
        if _points_of_sale:
            for pos in _points_of_sale:
                yield pos
        else:
            if _points_of_sale is None:
                logger.warning(f"{self.__class__.__name__}._points_of_sale: none because no 'sucursales' key in response from {url}")
                for key, value in response.items():
                    logger.warning(f"  {key}: {value}")
            elif isinstance(_points_of_sale, list):
                if len(_points_of_sale) == 0:
                    logger.warning(f"{self.__class__.__name__}._points_of_sale: none because len(list) == 0 in response from {url}")
                else:
                    logger.warning(f"{self.__class__.__name__}._points_of_sale: none because len(list) != 0 in response from {url}. This is weird.")
            else:
                logger.warning(f"{self.__class__.__name__}._points_of_sale: none because no way error from {url}. This is very weird.")
            raise ScraperStop()

    def points_of_sale(self) -> Generator[dict[str, Any], None, None]:
        """
        Fetches all points of sale (supermarkets) from the API, handling pagination.

        Yields:
            dictionaries, each representing a point of sale.
        """
        logger.info(f"{self.__class__.__name__}.points_of_sale: Starting to fetch points of sale.")
        initial_url = f"{self.base_url}{self._points_of_sale_endpoint}?limit=0"
        query_limits = self._get_query_limits(initial_url)

        if query_limits.total_items == 0:
            logger.warning(f"{self.__class__.__name__}.points_of_sale: No points of sale found.")
            yield from []
            return # Stop iteration because no items
        for page in range(query_limits.full_pages):
            offset = query_limits.offset(page)
            limit = query_limits.full_page_queries
            url = self._build_url(self._points_of_sale_endpoint, offset, limit)
            logger.info(f"{self.__class__.__name__}.points_of_sale: Fetching points of sale from {url}")
            for pos in self._points_of_sale(url):
                yield pos

        # Fetch items from the last page
        if query_limits.last_page_queries > 0:
            offset = query_limits.offset(query_limits.full_pages)
            limit = query_limits.last_page_queries
            url = self._build_url(self._points_of_sale_endpoint, offset, limit)
            logger.info(f"{self.__class__.__name__}.points_of_sale: Fetching last page of points of sale from {url}")
            for pos in self._points_of_sale(url):
                yield pos

        logger.info(f"{self.__class__.__name__}.points_of_sale: Finished fetching points of sale.")


    def articles_by_point_of_sale(self, point_of_sale_id: str) -> Generator[dict[str, Any], None, None]:
        """
        Fetches articles for a specific point of sale, handling pagination.

        Args:
            point_of_sale_id: The identifier of the point of sale.

        Yields:
            dictionaries, each representing an article with the point_of_sale_id added.

        Raises:
            ScraperError: If the initial query for limits fails for this point of sale.
        """
        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: Starting to fetch articles for point_of_sale_id {point_of_sale_id}")
        # Base URL for articles for this point of sale (without offset/limit)
        articles_base_url = f"{self.base_url}{self._articles_endpoint}"
        _params = {"id_sucursal": point_of_sale_id}

        initial_url = f"{articles_base_url}?id_sucursal={point_of_sale_id}&limit=0"
        query_limits = self._get_query_limits(initial_url)

        if query_limits.total_items == 0:
            logger.warning(f"{self.__class__.__name__}.articles_by_point_of_sale: No articles found for point_of_sale_id {point_of_sale_id}")
            yield from []
            return # Stop iteration if no items

        for page in range(query_limits.full_pages):
            offset = query_limits.offset(page)
            limit = query_limits.full_page_queries
            url = self._build_url(self._articles_endpoint, offset, limit, params=_params)
            logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: Fetching articles from {url}")
            response = self.webclient.get(url)
            _articles = response.get("productos", None)
            if _articles:
                 for article in _articles:
                     # Add point of sale identifier to each article
                     article["point_of_sale_id"] = point_of_sale_id
                     yield article
            else:
                yield from []
                logger.warning(f"{self.__class__.__name__}.articles_by_point_of_sale: No 'productos' key or empty list in response from {url}")
                for key, value in response.items():
                    logger.warning(f"  {key}: {value}")

        # Fetch items from the last page
        if query_limits.last_page_queries > 0:
            offset = query_limits.offset(query_limits.full_pages)
            limit = query_limits.last_page_queries
            url = self._build_url(self._articles_endpoint, offset, limit, params=_params)
            logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: Fetching last page of articles from {url}")
            response = self.webclient.get(url)
            _articles = response.get("productos", None)
            if _articles:
                 for article in _articles:
                    # Add point of sale identifier to each article
                    article["point_of_sale_id"] = point_of_sale_id
                    yield article
            else:
                yield from []
                logger.warning(f"{self.__class__.__name__}.articles_by_point_of_sale: No 'productos' key or empty list in response from {url}")
                for key, value in response.items():
                    logger.warning(f"  {key}: {value}")

        logger.info(f"{self.__class__.__name__}.articles_by_point_of_sale: Finished fetching articles for point_of_sale_id {point_of_sale_id}.")


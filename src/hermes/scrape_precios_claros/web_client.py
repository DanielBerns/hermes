import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional

from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

class WebClientError(Exception):
    """Custom exception class for WebClient errors."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class WebClient:
    """
    A client for making HTTP GET requests using the requests library.

    This client utilizes a `requests.Session` for potential performance benefits
    like connection pooling and cookie persistence across requests made with the
    same instance.

    It's recommended to use the `open_webclient` context manager to ensure
    the session is properly closed after use.

    Attributes:
        _session (Session): The underlying requests session object.
        _user_agent (str): The User-Agent string sent with requests.
    """

    def __init__(self, user_agent: str = "Mozilla/5.0") -> None:
        """
        Initializes the WebClient with a new requests Session.

        Args:
            user_agent (str): The User-Agent string to use for requests.
                               Defaults to "Mozilla/5.0".
        """
        self._session: Session = Session()
        self._user_agent: str = user_agent
        logger.info(f"{self.__class__.__name__} initialized.")

    def close(self) -> None:
        """
        Closes the underlying requests Session to release resources.
        This method is automatically called when using the `open_webclient`
        context manager or exiting a 'with' block.
        """
        if self._session:
            self._session.close()
            logger.info(f"{self.__class__.__name__} session closed.")
            # self._session = None # Optional: prevent reuse after close

    def get(self, url: str, params: Optional[dict[str, Any]] = None, timeout: int = 30) -> dict[str, Any]:
        """
        Performs an HTTP GET request to the specified URL.

        Args:
            url (str): The URL to send the GET request to.
            params (dict[str, Any], optional): Dictionary of URL parameters. Defaults to None.
            timeout (int): Request timeout in seconds. Defaults to 30.

        Returns:
            Dict[str, Any]: The JSON response content as a dictionary.

        Raises:
            WebClientError: If the request fails due to connection issues,
                             timeout, non-200 status code, or JSON decoding errors.
        """
        if not self._session:
             raise WebClientError(f"{self.__class__.__name__} session is closed. Cannot make requests.")

        headers = {"User-Agent": self._user_agent}
        logger.info(f"Attempting GET request to {url}")
        if params:
            logger.info(f"    with {params}")
        try:
            response = self._session.get(url, headers=headers, params=params, timeout=timeout)
            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()

            # Attempt to parse JSON response
            json_response = response.json()
            logger.info(f"GET request to {url} successful (Status: {response.status_code})")
            return json_response
        except RequestException as e:
            # Catches connection errors, timeouts, invalid URL, etc.
            error_message = f"Request failed for {url}: {e}"
            raise WebClientError(error_message)

        except ValueError as e: # Catches JSON decoding errors
             error_message = f"Failed to decode JSON response from {url}: {e}"
             raise WebClientError(error_message)


@contextmanager
def open_webclient(**kwargs) -> Generator[WebClient, None, None]:
    """
    Provides a context manager for safe handling of a WebClient instance.

    Ensures that the WebClient's session is properly closed upon exiting
    the 'with' block, even if errors occur.

    Args:
        **kwargs: Keyword arguments to pass to the WebClient constructor
                  (e.g., user_agent).

    Yields:
        WebClient: An initialized WebClient instance.

    Example:
        >>> try:
        ...     with open_webclient(user_agent="MyCustomAgent/1.0") as client:
        ...         data = client.get("https://httpbin.org/get")
        ...         print(data.get('headers', {}).get('User-Agent'))
        ... except WebClientError as e:
        ...     print(f"An error occurred: {e}")
        MyCustomAgent/1.0
    """
    client = None
    try:
        client = WebClient(**kwargs)
        yield client
    # No need to catch broad Exception here unless specific cleanup *before*
    # client.close() is needed. Let specific errors (like WebClientError) propagate.
    finally:
        if client:
            client.close()

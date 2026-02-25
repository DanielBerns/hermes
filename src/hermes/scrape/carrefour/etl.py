import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Protocol
from hermes.scrape.carrefour.models import ProductItem, ScrapedResult


class Extract(Protocol):

    @property
    def online_shop(self) -> str:
        ...

    @property
    def searches(self) -> List[str]:
        ...

    @property
    def store(self) -> Path:
        ...

    def target_html(self, index: int) -> Path:
        ...

    def execute(self, path_to_driver: str, path_to_browser: str, headless: bool = False) -> None:
        ...


class Transform(Protocol):
    def process_folder(self) -> List[ScrapedResult]:
        """Iterates over all HTML files in the folder and extracts data."""
        ...

    def _extract_from_file(self, filepath: Path) -> List[ScrapedResult]:
        """Extracts and parses JSON-LD content from a single HTML file."""
        ...

    def _parse_schema_data(self, data: Dict[str, Any], filename: str) -> List[ScrapedResult]:
        """Maps raw JSON dictionary into validated Pydantic models."""
        ...

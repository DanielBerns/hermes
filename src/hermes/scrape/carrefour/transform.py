# https://www.twz.com/news-features/the-massive-questions-surrounding-a-major-american-air-war-against-iran

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from hermes.scrape.carrefour.models import ProductItem, ScrapedResult

# Configure the module logger
logger = logging.getLogger(__name__)

class CarrefourTransform:
    def __init__(self, target_dir: Union[str, Path]):
        self.target_dir = Path(target_dir)
        if not self.target_dir.is_dir():
            logger.error(f"Initialization failed: '{self.target_dir}' is not a valid directory.")
            raise NotADirectoryError(f"Directory not found: {self.target_dir}")
        logger.info(f"Extractor initialized for directory: {self.target_dir}")

    def execute(self) -> List[ScrapedResult]:
        """Iterates over all HTML files in the folder and extracts data."""
        logger.info(f"Scanning '{self.target_dir}' for HTML files...")
        html_files = list(self.target_dir.rglob("*.html"))
        logger.info(f"Found {len(html_files)} HTML files to process.")

        all_results = []
        for file_path in html_files:
            logger.debug(f"Starting extraction for file: {file_path.name}")
            file_results = self._extract_from_file(file_path)
            all_results.extend(file_results)
            logger.debug(f"Finished extracting {len(file_results)} items from {file_path.name}")

        logger.info(f"Folder processing complete. Extracted a total of {len(all_results)} items.")
        return all_results

    def _extract_from_file(self, filepath: Path) -> List[ScrapedResult]:
        """Extracts and parses JSON-LD content from a single HTML file."""
        extracted_items = []
        head = 'ld+json'
        segment = '{"@context":"https://schema.org"'
        tail = '</script>'
        start = 0

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                raw = f.read()
                logger.debug(f"File '{filepath.name}' read successfully ({len(raw)} chars).")

                while start >= 0:
                    index = raw.find(head, start)
                    if index == -1:
                        logger.debug(f"No further '{head}' tags found in {filepath.name}.")
                        break

                    index = raw.find(segment, index)
                    if index == -1:
                        logger.debug(f"No schema.org segment found after '{head}' in {filepath.name}.")
                        break
                    else:
                        start = index

                    index = raw.find(tail, index)
                    if index == -1:
                        logger.warning(f"Malformed HTML in {filepath.name}: Missing closing script tag.")
                        break
                    else:
                        end = index

                    json_string = raw[start:end]

                    try:
                        data = json.loads(json_string)
                        logger.info(f"Successfully parsed JSON-LD block from {filepath.name}.")
                        items = self._parse_schema_data(data, filepath.name)
                        extracted_items.extend(items)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decoding failed in {filepath.name} at position {start}: {e}")

                    # Advance the pointer to find the next JSON-LD block
                    start = end

        except IOError as e:
            logger.error(f"Failed to read file {filepath}: {e}", exc_info=True)

        return extracted_items

    def _parse_schema_data(self, data: Dict[str, Any], filename: str) -> List[ScrapedResult]:
        """Maps raw JSON dictionary into validated Pydantic models."""
        results = []
        item_list = data.get('itemListElement', [])
        logger.debug(f"Processing {len(item_list)} elements found in itemListElement.")

        for element in item_list:
            position = element.get('position', 'unknown')
            item_data = element.get('item')

            if isinstance(item_data, dict):
                try:
                    # # Validate and parse the raw dictionary using Pydantic
                    product = ProductItem(**item_data)
                    result = ScrapedResult(
                        source_file=filename,
                        position=position,
                        product=product
                    )
                    results.append(result)
                    logger.debug(f"Successfully parsed item at position {position}: {product.name}")
                except Exception as e:
                    logger.warning(f"Validation error for item at position {position} in {filename}: {e}")
            elif isinstance(item_data, str):
                logger.debug(f"Item at position {position} is a direct URL/string, skipping detailed parse.")
            else:
                logger.warning(f"Unexpected item format at position {position} in {filename}.")

        return results

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Tuple, List

from hermes.core.tree_store import TreeStore
from hermes.core.helpers import get_resource, create_text_file
from hermes.domain.sample_reader import SampleReader


class TreeStoreStats:
    def __init__(self) -> None:
        self._table: List[dict[str, Any]] = []

    def add(self, record: dict[str, Any]) -> None:
        self._table.append(record)

    def report(self, container: Path, identifier: str) -> None:
        resource = get_resource(container, identifier, ".md")
        with create_text_file(resource) as text:
            text.write(f"# TreeStore stats {identifier}\n\n")
            text.write(f"## Records: {len(self._table)}\n\n")
            text.write("## Table\n\n")
            for number, record in enumerate(self._table):
                text.write(f"* {number}\n")
                for key, value in record.items():
                    text.write(f"    {key}: {value}\n")
                text.write("\n")


class SampleStats:
    def __init__(self) -> None:
        self._records: int = 0
        self._table: defaultdict[str, Counter[str]] = defaultdict(Counter)

    def add(self, record: dict[str, Any]) -> None:
        self._records += 1
        for key, value in record.items():
            self._table[key][str(value)] += 1

    def report(self, container: Path, identifier: str) -> None:
        resource = get_resource(container, identifier, ".md")
        with create_text_file(resource) as text:
            text.write(f"# Sample stats {identifier}\n\n")
            text.write(f"## Records: {self._records}\n\n")
            text.write("## Table\n\n")
            for first_key, values in self._table.items():
                text.write(f"### {first_key}\n\n")
                for second_key, number in values.items():
                    text.write(f"* {second_key}: {number}\n")
                text.write("\n\n")


def get_scrape_stats(
    tree_store: TreeStore,
) -> Tuple[TreeStoreStats, SampleStats, SampleStats]:
    tree_store_stats = TreeStoreStats()
    point_of_sale_stats = SampleStats()
    article_stats = SampleStats()
    for number, this_sample in enumerate(tree_store.iterate()):
        reader = SampleReader(this_sample)
        for this_point_of_sale in reader.points_of_sale():
            point_of_sale_stats.add(this_point_of_sale)
        for this_article in reader.articles_by_point_of_sale():
            article_stats.add(this_article)
        reader.metadata.read()
        metadata_table = reader.metadata.table
        timestamp = metadata_table.get("timestamp", "00000000000000")

        record = {
            "number": number,
            "index": this_sample.index,
            "key": this_sample.key,
            "home": this_sample.home,
            "check": number == this_sample.index,
            "timestamp": timestamp,
        }
        for key, value in metadata_table.items():
            record[f"metadata-{key}"] = value

        tree_store_stats.add(record)
    return tree_store_stats, point_of_sale_stats, article_stats

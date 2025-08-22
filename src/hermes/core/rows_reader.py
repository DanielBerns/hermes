from pathlib import Path
from typing import Any, Generator

from hermes.core.helpers import get_resource, read_json_lines


class RowsReader:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    @property
    def directory(self) -> Path:
        return self._directory

    def execute(self, identifier: str) -> Generator[dict[str, Any], None, None]:
        resource = get_resource(
            self.directory, identifier, ".jsonl"
        )  # TODO: refactor Rows, Samples, and so on...
        for row in read_json_lines(resource):
            yield row

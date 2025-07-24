from typing import Any, Generator
from pathlib import Path

from unpsjb_fce_obsecon.utils.helpers import read_json_lines, get_resource


class RowsReader:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    @property
    def directory(self) -> Path:
        return self._directory

    def execute(
        self,
        identifier: str
    ) -> Generator[dict[str, Any], None, None]:
        resource = get_resource(self.directory, identifier, ".jsonl") # TODO: refactor Rows, Samples, and so on...
        for row in read_json_lines(resource):
            yield row

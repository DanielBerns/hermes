from collections.abc import Callable
from pathlib import Path
from typing import Any, Generator

from hermes.core.formatter import Formatter
from hermes.core.helpers import create_text_file, get_resource


class RowsWriter:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    @property
    def directory(self) -> Path:
        return self._directory

    def execute(
        self,
        identifier: str,
        generator: Callable[[], Generator[dict[str, Any], None, None]],
        formatter: Formatter,
    ) -> None:
        resource = get_resource(self.directory, identifier, formatter.extension)
        with create_text_file(resource) as text:
            for row in generator():
                line = formatter.row_to_line(row)
                text.write(line)

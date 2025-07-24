from typing import Any, Generator
from pathlib import Path
from collections.abc import Callable

from unpsjb_fce_obsecon.utils.helpers import create_text_file, get_resource
from unpsjb_fce_obsecon.utils.formatter import Formatter


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
        formatter: Formatter
    ) -> None:
        resource = get_resource(self.directory, identifier, formatter.extension)
        with create_text_file(resource) as text:
            for record in generator():
                line = formatter.line(record)
                text.write(line)


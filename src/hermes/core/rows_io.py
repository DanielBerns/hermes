from pathlib import Path
from typing import Any, Generator

from hermes.core.helpers import get_resource, read_json_lines, write_text_lines

ROWS_SUFFIX = ".jsonl"

def read_rows(
    container: Path,
    identifier: str
) -> Generator[dict[str, Any], None, None]:
    resource = get_resource(
        container, identifier, ROWS_SUFFIX
    )
    yield from read_json_lines(resorce)


def write_rows(
    container: Path,
    identifier: str,
    generate_rows: Callable[[], Generator[dict[str, Any], None, None]],
) -> None:
    resource = get_resource(self.directory, identifier, ROWS_SUFFIX)

    def generate_lines() -> Generator[str, None, None]:
        for row in generate_rows():
            yield json.dumps(row)

    write_text_lines(resource, generate_lines)


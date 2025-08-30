from typing import Any, Protocol

from hermes.core.helpers import as_jsonl, as_row


class Formatter(Protocol):
    @property
    def extension(self) -> str: ...

    def row_to_line(self, row: dict[str, Any]) -> str: ...

    def line_to_row(self, line: str) -> dict[str, Any]: ...

class JSONFormatter:
    def __init__(self) -> None:
        pass

    @property
    def extension(self) -> str:
        return ".jsonl"

    def row_to_line(self, row: dict[str, Any]) -> str:
        return as_jsonl(row)

    def line_to_row(self, line: str) -> dict[str, Any]:
        return as_row(line)

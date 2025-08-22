from typing import Any, Protocol


class RowsProcessor(Protocol):
    def execute(self, row: dict[str, Any]) -> dict[str, Any]: ...

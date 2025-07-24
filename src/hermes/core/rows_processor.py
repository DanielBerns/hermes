from typing import Protocol, Any

class RowsProcessor(Protocol):
    def execute(self, row: dict[str, Any]) -> dict[str, Any]:
        ...

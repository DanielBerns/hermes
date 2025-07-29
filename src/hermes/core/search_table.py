from typing import Any

class SearchTable[T]:
    def __init__(self, identifier: str) -> None:
        self._identifier = identifier
        self._table: dict[str, T] = {}

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def table(self) -> dict[str, T]:
        return self._table

    def search(self, record: dict[str, Any]) -> T | None:
        key = record.get(self.identifier, None)
        value = self.table.get(str(key), None) if key else None
        return value

    def update(self, table_update: dict[str, T]) -> None:
        self._table.update(table_update)

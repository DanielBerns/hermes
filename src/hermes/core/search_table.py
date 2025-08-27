from typing import Any, List

class SearchTable:
    def __init__(
        self,
        keys: List[str]
    ) -> None:
        self.sorted_keys = sorted(keys)
        self._table: dict[str, dict[str, Any]] = {}

    def get_key(self, row: dict[str, Any]) -> str:
        return "|".join(str(row.get(key, '')) for key in self.sorted_keys)

    def insert(self, row: dict[str, Any]) -> None:
        key = self.get_key(row)
        self._table[key] = row

    def search(self, row: dict[str, Any]) -> dict[str, Any] | None:
        key = self._get_key(row)
        return self._table.get(key, None)

    def iterate(self) -> Generator[dict[str, Any], None, None]:
        for key, value in self._table.items():
            yield value

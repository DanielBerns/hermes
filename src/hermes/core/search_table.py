from typing import Any, List, Generator

class SearchTable:
    def __init__(
        self,
        keys: List[str]
    ) -> None:
        self.sorted_keys = sorted(keys)
        self._table: dict[str, dict[str, Any]] = {}

    def get_key(self, row: dict[str, Any]) -> str:
        return "|".join(str(row.get(key, '')) for key in self.sorted_keys)

    def insert(self, row: dict[str, Any]) -> bool:
        key = self.get_key(row)
        if self._table.get(key):
            return False
        else:
            self._table[key] = True
            return True

    def search(self, row: dict[str, Any]) -> bool:
        key = self._get_key(row)
        return self._table.get(key, False)

    def iterate(self) -> Generator[dict[str, Any], None, None]:
        for key, value in self._table.items():
            yield value

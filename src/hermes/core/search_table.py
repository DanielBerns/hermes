from typing import Any, List, Generator

class SearchTable:
    def __init__(
        self,
        keys: List[str]
    ) -> None:
        self.sorted_keys = sorted(keys)
        self._table: dict[str, bool] = {}

    def get_key(self, row: dict[str, Any]) -> str:
        return "|".join(str(row.get(key, '')) for key in self.sorted_keys)

    def insert(self, row: dict[str, Any]) -> bool:
        key = self.get_key(row)
        if self._table.get(key):
            return False
        else:
            # Store a boolean flag to indicate presence, not the whole row
            self._table[key] = True
            return True

    def search(self, row: dict[str, Any]) -> bool:
        # Corrected the typo from _get_key to get_key
        key = self.get_key(row)
        return self._table.get(key, False)


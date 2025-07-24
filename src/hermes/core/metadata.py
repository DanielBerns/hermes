from pathlib import Path
from typing import Any
from unpsjb_fce_obsecon.utils.helpers import read_json, write_json, get_resource

class Metadata:
    def __init__(self, directory: Path, filename: str = "metadata") -> None:
        self._table: dict[str, Any] = {}
        self._resource: Path = get_resource(directory, filename, ".json")

    @property
    def table(self) -> dict[str, Any]:
        return self._table

    @table.setter
    def table(self, value: dict[str, Any]):
        self._table = value

    @property
    def resource(self) -> Path:
        return self._resource

    def add(self, key: str, value: Any) -> None:
        self.table[key] = value

    def read(self):
        data = read_json(self.resource)
        self.table.update(data)

    def write(self) -> None:
        write_json(self.resource, self.table)


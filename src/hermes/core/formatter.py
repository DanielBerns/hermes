from typing import Protocol, Any

from unpsjb_fce_obsecon.utils.helpers import as_jsonl


class Formatter(Protocol):
    @property
    def extension(self) -> str:
        ...

    def line(self, row: dict[str, Any]) -> str:
        ...


class JSONFormatter:
    def __init__(self) -> None:
        pass

    @property
    def extension(self) -> str:
        return ".jsonl"

    def line(self, row: dict[str, Any]) -> str:
        return as_jsonl(row)

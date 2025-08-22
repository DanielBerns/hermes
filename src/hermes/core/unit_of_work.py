from typing import Any, Self


class UnitOfWorkBuilder:
    def init(self) -> None:
        self._identifier: str | None = None
        self._parameters: dict[str, Any] = {}

    def set_identifier(self, value: str) -> Self:
        self._identifier = value
        return self

    def add_parameter(self, key: str, value: Any) -> Self:
        self._parameters[key] = value
        return self

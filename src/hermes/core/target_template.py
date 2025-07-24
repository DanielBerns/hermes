from jinja2 import Template
from pathlib import Path
from typing import Protocol


class TargetTemplate(Protocol):
    @property
    def template(self) -> Template:
        ...

    @property
    def suffix(self) -> str:
        ...


class FromFileTargetTemplate:
    def __init__(self, resource: Path) -> None:
        self._resource = resource

    @property
    def resource(self) -> Path:
        return self._resource

    @property
    def template(self) -> Template:
        with open(self.resource, "r") as origin:
            return Template(origin.read())

    @property
    def suffix(self) -> str:
        return self.resource.suffix


class FromStringTargetTemplate:
    def __init__(self, string: str, suffix: str = ".txt") -> None:
        self._template = Template(string)
        self._suffix = suffix

    @property
    def template(self) -> Template:
        return self._template

    @property
    def suffix(self) -> str:
        return self._suffix

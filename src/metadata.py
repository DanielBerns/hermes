import logging
from pathlib import Path
from typing import Any
from helpers import read_json, write_json, get_container, get_resource

logger = logging.getLogger(__name__)


class MetadataException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Metadata:
    def __init__(self, base: Path, name: str = ".metadata") -> None:
        self._container: Path = get_container(base, name)

    @property
    def container(self) -> Path:
        return self._container

    def read_tool(self, tool: str) -> dict[str, Any]:
        try:
            resource = get_resource(self.container, tool, '.json')
            return read_json(self.resource)
        except Exception as an_exception:
            message = f"{self.__class__.__name__}.read_tool({tool})"
            raise MetadataException(message) from an_exception

    def read(self) -> dict[str, Any]:
        tools = {}
        for name in os.listdir(self.container):
            a_path = Path(self.container, name)
            if a_path.is_file():
                if a_path.suffix == '.json':
                    parameters = read_json(a_path)
                    key = name[:-5] # name[:-len(".json")]
                    tools[key] = parameters
        return tools

    def write_tool(self, tool: str, parameters: dict[str, Any]) -> None:
        resource = get_resource(self.container, tool, '.json')
        write_json(resource, parameters)

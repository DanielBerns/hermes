import logging
from pathlib import Path
from typing import Any

from hermes.core.constants import INSTANCE, INSTANCE_BASE, PROJECT
from hermes.core.helpers import get_container

# Get a named logger for this module
logger = logging.getLogger(__name__)

class StorageException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Storage:
    def __init__(self, config: dict[str, Any]) -> None:
        instance_base = config.get(INSTANCE_BASE, None)
        if instance_base is None:
            project = config.get(PROJECT, "Horror")
            instance = config.get(INSTANCE, "Hell")
            message = f"{self.__class__.__name__}.__init__: {project}, {instance} - null instance_base"
            raise StorageException(message)
        self._base = instance_base

    @property
    def base(self) -> Path:
        return self._base

    def container(self, identifier: str, base: Path | None = None) -> Path:
        return get_container(base, identifier) if base else get_container(self.base, identifier)

    @property
    def parameters_container(self) -> Path:
        return self.container("parameters")

    @property
    def secrets_container(self) -> Path:
        return self.container(".secrets")

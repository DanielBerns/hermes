import logging
from pathlib import Path
from typing import Any

from hermes.core.helpers import get_container

# Get a named logger for this module
logger = logging.getLogger(__name__)


class StorageException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Storage:
    def __init__(self, base: Path) -> None:
        self._base = base

    @property
    def base(self) -> Path:
        return self._base

    def container(self, identifier: str, base: Path | None = None) -> Path:
        return get_container(base, identifier) if base else get_container(self.base, identifier)

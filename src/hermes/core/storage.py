import logging
from pathlib import Path
from typing import Any

from hermes.core.constants import INFO_BASE_KEY, SECRETS_BASE_KEY
from hermes.core.helpers import get_container

# Get a named logger for this module
logger = logging.getLogger(__name__)


class StorageException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Storage:
    def __init__(self, config: dict[str, Any]) -> None:
        info_base = config.get(INFO_BASE_KEY, None)
        secrets_base = config.get(SECRETS_BASE_KEY, None) if info_base else None
        if secrets_base is None:
            message = f"{self.__class__.__name__}.__init__: wrong config"
            raise StorageException(message)
        self._info_base = info_base
        self._secrets_base = secrets_base

    @property
    def info_base(self) -> Path:
        return self._info_base

    def container(self, identifier: str, base: Path | None = None) -> Path:
        return (
            get_container(base, identifier)
            if base
            else get_container(self.info_base, identifier)
        )

    @property
    def parameters_container(self) -> Path:
        return self.container("parameters")

    @property
    def secrets_base(self) -> Path:
        return self._secrets_base

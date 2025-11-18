import logging
import sys
from pathlib import Path
from typing import Any, Protocol

from hermes.core.cli import CLI
from hermes.core.config import config
from hermes.core.constants import (
    INFO,
    SECRETS,
    LOGS,
    MESSAGE_BOARD,
    INSTANCE_KEY,
    DATABASE_NAME_KEY,
    YES,
    NO,
    info_root,
    secrets_root
)
from hermes.core.helpers import get_directory, get_resource, get_timestamp
from hermes.core.storage import Storage



class Infra:
    def __init__(
        self,
        script: str,
        project_identifier: str
    ) -> None:
        self._script: str = script
        self._project_identifier: str = project_identifier
        self._instance: str = config.project.instance
        version = config.project.version
        self._semantic_version: str = f"{version[0]}.{version[1]}"
        self._database_name: str = config.database.name
        self._info_base: Path = get_directory(info_root / self.project_identifier / self.semantic_version / self.instance)
        self._info_storage: Storage = Storage(self.info_base)
        self._logging_level: str = config.logging.level
        self._timestamped: bool = config.logging.timestamped
        self._logs_container: Path = self.info_storage.container(LOGS)
        self._logs_name: str = f"{script}_{get_timestamp()}" if self.timestamped else self.script
        self.configure_logging()

        self._secrets_base: Path = get_directory(secrets_root / self.project_identifier / self.semantic_version / self.instance)
        self._secrets_storage: Storage = Storage(self.secrets_base)
        self._message_board_enabled: bool = config.message_board.enabled
        self._message_board_identifier: str = config.message_board.identifier
        self._message_board_base: Path = self.secrets_storage.container(MESSAGE_BOARD)

    @property
    def script(self) -> str:
        return self._script

    @property
    def project_identifier(self) -> str:
        return self._project_identifier

    @property
    def instance(self) -> str:
        return self._instance

    @property
    def semantic_version(self) -> str:
        return self._semantic_version

    @property
    def database_name(self) -> str:
        return self._database_name

    @property
    def info_base(self) -> str:
        return self._info_base

    @property
    def info_storage(self) -> Storage:
        return self._info_storage

    @property
    def logging_level(self) -> str:
        return self._logging_level

    @property
    def timestamped(self) -> bool:
        return self._timestamped

    @property
    def logs_container(self) -> Path:
        return self._logs_container

    @property
    def logs_name(self) -> Path:
        return self._logs_name

    @property
    def secrets_base(self) -> str:
        return self._secrets_base

    @property
    def secrets_storage(self) -> Storage:
        return self._secrets_storage

    @property
    def message_board_enabled(self) -> bool:
        return self._message_board_enabled

    @property
    def message_board_identifier(self) -> str:
        return self._message_board_identifier

    @property
    def message_board_base(self) -> Path:
        return self._message_board_base

    def configure_logging(
        self,
    ) -> None:
        # 1. Get the root logger
        root_logger = logging.getLogger()  # Getting the root logger by not providing a name

        # Set the minimum level for the root logger to process messages
        # Messages below this level (e.g., DEBUG if level is INFO) will be ignored by this logger and its handlers
        root_logger.setLevel(self.logging_level)

        # Ensure the root logger doesn"t already have handlers to avoid duplicate messages
        if not root_logger.handlers:
            # 2. Create a handler
            # Handlers send log records to specific destinations (console, file, etc.)
            console_handler = logging.StreamHandler(sys.stdout)  # Send logs to the console

            # Set the minimum level for THIS HANDLER
            # The handler will only process messages that the logger PASSED and are at or above its level
            console_handler.setLevel(
                logging.WARNING
            )  # Only show WARNING level messages and above on console

            # 3. Create a formatter
            # Formatters specify the layout of the log record
            formatter = logging.Formatter(
                "%(name)s - %(levelname)s - %(message)s"
            )

            # 4. Add formatter to handler
            console_handler.setFormatter(formatter)

            # 5. Add handler to the root logger
            root_logger.addHandler(console_handler)

            # --- Add a File Handler ---
            logs_txt = get_resource(self.logs_container, self.logs_name, ".txt")
            file_handler = logging.FileHandler(logs_txt)
            file_handler.setLevel(self.logging_level)  # Configurable log_level
            file_handler.setFormatter(formatter)  # Use the same formatter
            root_logger.addHandler(file_handler)

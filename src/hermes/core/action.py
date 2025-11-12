import logging
import sys
from pathlib import Path
from typing import Any, Protocol

from hermes.message_board.agent import initialize_agent, SendPublicMessage

from hermes.core.config import get_config

from hermes.core.constants import (
     INFO,
     SECRETS,
     LOGS,
     MESSAGE_BOARD,
     info_root,
     secrets_root
)

from hermes.core.helpers import get_directory, get_resource, get_timestamp
from hermes.core.storage import Storage


class Action(Protocol):
    def run(self, info_storage: Storage, secrets_storage: Storage) -> None: ...


def configure_logging(
    logs_container: Path,
    logs_name: str,
    logging_level: str
) -> None:
    logs_txt = get_resource(logs_container, logs_name, ".txt")

    # 1. Get the root logger
    root_logger = logging.getLogger()  # Getting the root logger by not providing a name

    # Set the minimum level for the root logger to process messages
    # Messages below this level (e.g., DEBUG if level is INFO) will be ignored by this logger and its handlers
    root_logger.setLevel(logging_level)  # Process DEBUG level messages and above

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
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 4. Add formatter to handler
        console_handler.setFormatter(formatter)

        # 5. Add handler to the root logger
        root_logger.addHandler(console_handler)

        # --- Add a File Handler ---
        file_handler = logging.FileHandler(logs_txt)
        file_handler.setLevel(logging_level)  # Configurable log_level
        file_handler.setFormatter(formatter)  # Use the same formatter
        root_logger.addHandler(file_handler)


def execute(script: str, project_identifier: str, this_action: Action) -> None:

    config = get_config()

    project_identifier = config.project.identifier
    instance = config.project.instance
    version = config.project.version
    semantic_version = f"{version[0]}.{version[1]}"
    database_name = config.database.name
    logging_level = config.logging.level
    timestamped = config.logging.timestamped

    info_base = get_directory(info_root / project_identifier / semantic_version / instance)
    secrets_base = get_directory(secrets_root / project_identifier / semantic_version / instance)
    info_storage = Storage(info_base)
    secrets_storage = Storage(secrets_base)

    logs_container = info_storage.container(LOGS)
    logs_name = f"{script}_{get_timestamp()}" if timestamped else script
    configure_logging(logs_container, logs_name, logging_level)
    logger = logging.getLogger(__name__)
    logger.info("start.")

    message_board_enabled = config.message_board.enabled
    action_name = this_action.__class__.__name__
    message_board_base = secrets_storage.container(MESSAGE_BOARD)
    message_board_identifier = config.message_board.identifier
    try:
        if message_board_enabled:
            with initialize_agent(message_board_base, message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "start"], f"{action_name} start {get_timestamp()}"
                )
                agent.add(message)
        this_action.run(info_storage, secrets_storage)
        if message_board_enabled:
            with initialize_agent(message_board_base, message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "done"], f"{action_name} done {get_timestamp()}"
                )
                agent.add(message)
    except Exception as message:
        logger.error(f"{message}")
        logger.error(f"{script} failed.")
        if message_board_enabled:
            with initialize_agent(message_board_base, message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "error"], f"{action_name} failed {get_timestamp()}"
                )
                agent.add(message)
    else:
        logger.info(f"{script}  done.")

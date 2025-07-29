import logging
import sys
from pathlib import Path
from typing import Any, Protocol

from hermes.core.bot import SendPublicMessage, initialize_bot

from hermes.core.cli import CLI
from hermes.core.config import load_config
from hermes.core.constants import (
    INFO,
    INSTANCE,
    INSTANCE_DEFAULT,
    LOG_LEVEL,
    LOGS,
    MESSAGE_BOARD
)
from hermes.core.helpers import get_directory, get_resource, get_timestamp
from hermes.core.storage import Storage


class Action(Protocol):
    def configure(self, this_cli: CLI) -> None:
        ...

    def run(self, script: str, arguments: dict[str, Any], config: dict[str, Any], storage: Storage) -> None:
        ...


def configure_logging(
    script: str,
    arguments: dict[str, Any],
    config: dict[str, Any],
    storage: Storage
) -> None:
    logs_container = storage.container(LOGS)
    logs_txt = get_resource(logs_container, script, ".txt")
    log_level = config.get(LOG_LEVEL, logging.DEBUG)

    # 1. Get the root logger
    root_logger = logging.getLogger() # Getting the root logger by not providing a name

    # Set the minimum level for the root logger to process messages
    # Messages below this level (e.g., DEBUG if level is INFO) will be ignored by this logger and its handlers
    root_logger.setLevel(logging.DEBUG) # Process DEBUG level messages and above

    # Ensure the root logger doesn't already have handlers to avoid duplicate messages
    if not root_logger.handlers:
        # 2. Create a handler
        # Handlers send log records to specific destinations (console, file, etc.)
        console_handler = logging.StreamHandler(sys.stdout) # Send logs to the console

        # Set the minimum level for THIS HANDLER
        # The handler will only process messages that the logger PASSED and are at or above its level
        console_handler.setLevel(logging.WARNING) # Only show WARNING level messages and above on console

        # 3. Create a formatter
        # Formatters specify the layout of the log record
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # 4. Add formatter to handler
        console_handler.setFormatter(formatter)

        # 5. Add handler to the root logger
        root_logger.addHandler(console_handler)

        # --- Add a File Handler ---
        file_handler = logging.FileHandler(logs_txt)
        file_handler.setLevel(log_level) # Configurable log_level
        file_handler.setFormatter(formatter) # Use the same formatter
        root_logger.addHandler(file_handler)



def execute(
    script: str,
    project_identifier: str,
    this_action: Action
) -> None:
    info_directory = get_directory(Path.home() / INFO)

    my_cli = CLI(description="Download data from precios_claros@mecon for a given region.")
    my_cli.add_argument(
        '--instance',
        choices=['Patagonia', 'noA', 'nEa', "Centro"],             # Define the allowed choices
        default='Patagonia',                                       # Set a default value (optional)
        help="Set the name of the instance (choices: %(choices)s)" # Use %(choices)s to list options in help
    )
    my_cli.add_argument(
        '--message_board',
        choices=["yes", "no"],             # Define the allowed choices
        default='no',                      # Set a default value (optional)
        help="use message_board server (choices: %(choices)s)" # Use %(choices)s to list options in help
    )
    this_action.configure(my_cli)
    arguments = my_cli.arguments
    instance = arguments.get(INSTANCE, INSTANCE_DEFAULT)
    message_board = arguments.get(MESSAGE_BOARD, "no") == "yes"

    config = load_config(info_directory, project_identifier, instance)
    storage = Storage(config)

    configure_logging(script, arguments, config, storage)
    logger = logging.getLogger(__name__)
    logger.info(f"{script} start.")

    secrets_container = storage.secrets_container
    identifier = this_action.__class__.__name__
    try:
        if message_board:
            with initialize_bot(secrets_container, identifier) as bot:
                message = SendPublicMessage(
                    ["pipeline", "start"],
                    f"{identifier} start {get_timestamp()}"
                )
                bot.add(message)
        this_action.run(script, arguments, config, storage)
        if message_board:
            with initialize_bot(secrets_container, identifier) as bot:
                message = SendPublicMessage(
                    ["pipeline", "done"],
                    f"{identifier} done {get_timestamp()}"
                )
                bot.add(message)
    except Exception as message:
        logger.error(f"{message}")
        logger.error(f"{script} failed.")
        if message_board:
            with initialize_bot(secrets_container, identifier) as bot:
                message = SendPublicMessage(
                    ["pipeline", "error"],
                    f"{identifier} failed {get_timestamp()}"
                )
                bot.add(message)
    else:
        logger.info(f"{script}  done.")

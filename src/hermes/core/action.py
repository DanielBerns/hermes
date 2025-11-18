import logging
import sys
from pathlib import Path
from typing import Any, Protocol

from hermes.message_board.agent import initialize_agent, SendPublicMessage

from hermes.core.config import config

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


def execute(script: str, project_identifier: str, this_action: Action) -> None:

    infra = Infra(script, project_identifier)
    action_name = this_action.__class__.__name__
    event_name = f"{script}.{action_name}@{project_identifier}:"
    logger = logging.getLogger(__name__)
    logger.info(f"{event_name} start")

    try:
        if infra.message_board_enabled:
            with initialize_agent(infra.message_board_base, infra.message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "start"], f"{event_name} start {get_timestamp()}"
                )
                agent.add(message)
        this_action.run(info_storage, secrets_storage)
        if infra.message_board_enabled:
            with initialize_agent(infra.message_board_base, infra.message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "done"], f"{event_name} done {get_timestamp()}"
                )
                agent.add(message)
    except Exception as message:
        logger.error(f"{message}")
        logger.info(f"{event_name} failed {get_timestamp()}")

        if infra.message_board_enabled:
            with initialize_agent(message_board_base, message_board_identifier) as agent:
                message = SendPublicMessage(
                    ["pipeline", "error"], f"{script}.{action_name}@{project_identifier} failed {get_timestamp()}"
                )
                agent.add(message)
    else:
       logger.info(f"{event_name} done {get_timestamp}")


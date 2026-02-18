import logging
import sys
from pathlib import Path
from typing import Any, Protocol

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
from hermes.core.infra import Infra
from hermes.core.storage import Storage

class Action(Protocol):
    def run(self, info_storage: Storage, secrets_storage: Storage) -> None: ...


logger = logging.getLogger(__name__)

def execute(script: str, project_identifier: str, this_action: Action) -> None:
    # Initialize storage using infra
    infra = Infra(script, project_identifier)
    info_storage = infra.info_storage
    secrets_storage = infra.secrets_storage

    # Initialize names
    action_name = this_action.__class__.__name__
    event_name = f"{script}.{action_name}@{project_identifier}:"

    # Execution
    logger.info(f"{event_name} start")
    try:
        this_action.run(info_storage, secrets_storage)
        logger.info(f"{event_name} succeded {get_timestamp()}")
    except Exception as message:
        logger.error(f"{message}")
        logger.info(f"{event_name} failed {get_timestamp()}")
    else:
       logger.info(f"{event_name} done {get_timestamp()}")


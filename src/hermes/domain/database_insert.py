
import logging
import os
from typing import Any, Generator
from pathlib import Path

from hermes.core.cli import CLI
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore, Store
from hermes.core.helpers import get_container, get_resource, classname
from hermes.core.rows_reader import RowsReader
from hermes.domain.sample import Sample
from hermes.domain.sample_reader import SampleReader
from hermes.domain.database import get_session
from hermes.domain.database_from_sample import DatabaseFromSample
from hermes.core.constants import DATABASE_NAME_KEY


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class DatabaseUpdateException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class DatabaseInsert:

    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(
        self,
        script: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
        storage: Storage
    ) -> None:
        mecon_container = storage.container(Sample.MECON)
        tree_store = TreeStore(mecon_container, Sample.TREE_STORE)
        database_container = storage.container(Sample.DATABASE, base=mecon_container)
        database_name = arguments.get(DATABASE_NAME_KEY, "mecon_dev")
        database_uri = str(get_resource(database_container, database_name, ".db"))
        #   ()
        with get_session(database_uri) as session:
            db = DatabaseFromSample(session)
            for store in tree_store.iterate():
                try:
                    db.read_and_process_sample(store)
                except Exception as an_exception:
                    logger.error(f"{str(an_exception)}")
                    logger.warning(f"{self.__class__.__name__}.update: check {store.key} - {store.timestamp}")



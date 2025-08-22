import logging
from typing import Any

from hermes.core.cli import CLI
from hermes.core.helpers import get_resource
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore

from hermes.domain.sample import Sample
from hermes.domain.sample_reader import SampleReader

logger = logging.getLogger(__name__)


class SampleResetException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class SampleReset:
    def __init__(self) -> None:
        pass

    def configure(self, this_cli: CLI) -> None:
        pass

    def run(
        self,
        script: str,
        arguments: dict[str, Any],
        config: dict[str, Any],
        storage: Storage,
    ) -> None:
        mecon_container = storage.container(Sample.MECON)
        tree_store = TreeStore(mecon_container, Sample.DBPreciosClarosInterface)
        db_container = storage.container(Sample.DATABASE, base=mecon_container)
        db_name = config.get(Sample.DB_NAME, "mecon_dev")
        db_uri = str(get_resource(db_container, db_name, ".db"))
        for store in tree_store.iterate():
            try:
                reader = SampleReader(store.home)
                reader.metadata.read()
                reader.metadata.table = {}
                reader.metadata.table["key"] = store.key
                reader.metadata.table["timestamp"] = store.timestamp
                reader.metadata.table[db_uri] = False
                reader.metadata.write()
            except Exception as an_exception:
                logger.error(f"{str(an_exception)}")
                logger.warning(
                    f"{self.__class__.__name__}.update: check {store.key} - {store.timestamp}"
                )

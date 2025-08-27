import pdb
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


class DatabaseStatesGenerator:

    CODES_AND_STATES = {
      "ar-a": "Salta",
      "ar-b": "Buenos Aires",
      "ar-c": "CABA",
      "ar-d": "San Luis",
      "ar-e": "Entre Rios",
      "ar-f": "La Rioja",
      "ar-g": "Santiago del Estero",
      "ar-h": "Chaco",
      "ar-j": "San Juan",
      "ar-k": "Catamarca",
      "ar-l": "La Pampa",
      "ar-m": "Mendoza",
      "ar-n": "Misiones",
      "ar-p": "Formosa",
      "ar-q": "Neuquén",
      "ar-r": "Río Negro",
      "ar-s": "Santa Fé",
      "ar-t": "Tucumán",
      "ar-u": "Chubut",
      "ar-v": "Tierra del Fuego",
      "ar-w": "Corrientes",
      "ar-x": "Córdoba",
      "ar-y": "San Salvador de Jujuy",
      "ar-z": "Santa Cruz",
      "buenos aires": "Buenos Aires",
      "catamarca": "Catamarca",
      "chaco": "Chaco",
      "cordoba": "Córdoba",
      "corrientes": "Corrientes",
      "la pampa": "La Pampa",
      "la rioja": "La Rioja",
      "neuquen": "Neuquén",
      "rio negro": "Río Negro",
      "salta": "Salta",
      "san luis": "San Luis",
      "santa fe": "Santa Fe",
      "santiago del estero": "Santiago del Estero",
      "tucuman": "Tucumán",
      "caba": "CABA",
      "jujuy": "Jujuy",
      "formosa": "Formosa",
      "misiones": "Misiones",
      "capital federal": "CABA",
      "mendoza": "Mendoza",
      "error": "Error"
    }

    def __init__(
        self,
        reader: SampleReader,
    ) -> None:
      self._reader = reader

    @property
    def reader(self) -> SampleReader:
        return self._reader

    def iterate(self) -> Generator[dict[str, Any], None, None]:
        logger.info(f"{self.__class__.__name__}.iterate: start")
        included: dict[str, bool] = {}
        for row in self.reader.points_of_sale():
            state_code = row.get("state", False)
            state_name = DatabaseStatesGenerator.CODES_AND_STATES.get(state_code, False) if state_code else False
            if state_name:
                test = not included.get(state_code, False)
                if test:
                    included[state_code] = True
                    yield {"code": state_code, "name": state_name}
                else:
                    pass # We have seen this state code before.
            else:
                message = f"***{self.__class__.__name__}.iterate {row_to_string(row)}***"
                raise DatabaseUpdateException(message)
        logger.info(f"{self.__class__.__name__}.iterate: done {len(included)}")



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
        pdb.set_trace()
        with get_session(database_uri) as session:
            db = DatabaseFromSample(session)
            for store in tree_store.iterate():
                try:
                    db.read_and_process(store)
                    reader.metadata.read()
                    reader.metadata.table[db_uri] = True
                    reader.metadata.write()
                except Exception as an_exception:
                    logger.error(f"{str(an_exception)}")
                    logger.warning(f"{self.__class__.__name__}.update: check {store.key} - {store.timestamp}")



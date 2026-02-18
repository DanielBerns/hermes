import logging

from hermes.domain.sample import Sample
from hermes.core.rows_selector import RowsSelector
from hermes.core.storage import Storage
from hermes.precios_claros.context import get_precios_claros_context

logger = logging.getLogger(__name__)

class DatabasePreciosClarosStart:
    def __init__(self) -> None:
        pass

    def run(self, info_storage: Storage, secrets_storage: Storage) -> None:
        logger.info("Initializing Precios Claros parameters...")
        ctx = get_precios_claros_context(info_storage)
        RowsSelector.create(
            ctx.tree_store.parameters, Sample.STATES_AND_CITIES_SELECTOR, Sample.STATES_AND_CITIES
        )
        logger.info("Initialization complete.")


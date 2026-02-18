from dataclasses import dataclass
from typing import Generator
import logging

from hermes.core.config import config
from hermes.core.helpers import get_resource
from hermes.core.storage import Storage
from hermes.core.tree_store import TreeStore
from hermes.domain.sample import Sample

logger = logging.getLogger(__name__)

@dataclass
class PreciosClarosContext:
    tree_store: TreeStore
    db_uri: str
    mecon_container: Storage
    
def get_precios_claros_context(info_storage: Storage) -> PreciosClarosContext:
    """
    Encapsulates common setup for Precios Claros actions.
    """
    mecon_container = info_storage.container(Sample.MECON)
    tree_store = TreeStore(
        info_storage.container(Sample.TREE_STORE, base=mecon_container)
    )
    
    db_container = info_storage.container(Sample.DATABASE, base=mecon_container)
    db_name = config.database.name
    db_uri = str(get_resource(db_container, db_name, ".db"))
    
    return PreciosClarosContext(
        tree_store=tree_store,
        db_uri=db_uri,
        mecon_container=mecon_container
    )

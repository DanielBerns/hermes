# hermes-main/scripts/populate_tag_dictionary.py
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
from hermes.domain.models import TagDictionary, Base

# --- Configuration ---
TAG_HIERARCHY = {
    "ALMACEN": {},
    "BEBIDAS": {
        "CON ALCOHOL": {},
        "SIN ALCOHOL": {"GASEOSA": {}, "AGUA": {}},
    },
    "FRESCOS": {
        "LACTEOS": {
            "LECHE": {},
            "QUESO": {},
            "YOGUR": {},
        }
    },
}

filename = Path(__file__)
script, project_identifier = filename.stem, filename.parents[1].stem
services = Services(script, project_identifier)

# --- Logging
logger = logging.getLogger(__name__)
logger.info(f"Starting {script}")

# --- Paths to build db_uri
mecon_container = services.storage.container("mecon")
db_container = services.storage.container("database", base=mecon_container)
db_uri = str(get_resource(db_container, services.database_name, ".db"))


def populate_tags(session, hierarchy, parent=None):
    for tag_name, children in hierarchy.items():
        # Check if the tag already exists
        existing_tag = session.execute(
            select(TagDictionary).filter_by(tag_name=tag_name.upper())
        ).scalar_one_or_none()

        if not existing_tag:
            new_tag = TagDictionary(
                tag_name=tag_name.upper(),
                parent=parent,
            )
            session.add(new_tag)
            session.commit()  # Commit to get the ID for the children
            logger.info(f"Added tag: {tag_name.upper()}")
            populate_tags(session, children, new_tag)
        else:
            logger.info(f"Tag already exists: {tag_name.upper()}")
            populate_tags(session, children, existing_tag)


def main():
    """Main function to populate the tag dictionary."""
    logger.info("--- Starting Tag Dictionary Population ---")
    with get_session(db_uri) as db_session
        populate_tags(db_session, TAG_HIERARCHY)
        logger.info("--- Tag Dictionary Population Finished ---")


if __name__ == "__main__":
    main()


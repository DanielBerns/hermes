import logging
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

# Import the models from your database definition file
from database import (
    Base,
    Timestamp,
    State,
    City,
    Flag,
    Business,
    Branch,
    Place,
    PointOfSale,
    ArticleCode,
    Price,
    DatabaseException,
)

# --- Database Setup ---
# For demonstration, we'll use an in-memory SQLite database.
# In a real application, you would replace this with your actual database URL.
# Example for PostgreSQL: "postgresql://user:password@host/dbname"
DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(DATABASE_URL, echo=False)  # Set echo=True to see generated SQL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_database():
    """Creates all database tables."""
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created.")


# --- Generic Helper Functions ---


def get_or_create(
    session: Session, model, defaults: Dict[str, Any] = None, **kwargs
) -> Any:
    """
    Gets an object or creates it if it doesn't exist.
    This is useful for lookup tables like State, City, Flag, etc. to avoid duplicates.

    Args:
        session: The SQLAlchemy session object.
        model: The ORM model class.
        defaults: A dictionary of attributes to set if the object is created.
        **kwargs: The attributes to query for the object's existence.

    Returns:
        The existing or newly created object.
    """
    instance = session.execute(select(model).filter_by(**kwargs)).scalar_one_or_none()
    if instance:
        return instance
    else:
        params = {**kwargs, **(defaults or {})}
        instance = model(**params)
        session.add(instance)
        # We commit here to ensure the object gets an ID for subsequent operations
        # within the same transaction.
        session.commit()
        return instance


# --- CRUD Operations for Models ---

# --- Point of Sale and related entities ---


def bulk_insert_points_of_sale(session: Session, pos_data: List[Dict[str, Any]]):
    """
    Performs a bulk insert of PointOfSale records.
    This is highly efficient for large datasets.

    The function expects a list of dictionaries, where each dictionary represents
    a point of sale and its related entities.

    Example `pos_data` entry:
    {
        "code_and_flag": "XYZ-123",
        "flag_name": "MyFlag",
        "business_name": "MyBusiness",
        "branch_name": "Main Branch",
        "state_name": "MyState",
        "state_code": "MS",
        "city_name": "MyCity",
        "address": "123 Main St"
    }

    Args:
        session: The SQLAlchemy session object.
        pos_data: A list of dictionaries with point of sale data.
    """
    if not pos_data:
        return

    logger.info(f"Starting bulk insert for {len(pos_data)} points of sale.")

    # Step 1: Get or create all related entities to resolve foreign keys.
    # This avoids inserting one by one and leverages get_or_create.
    for data in pos_data:
        data["flag_id"] = get_or_create(session, Flag, name=data["flag_name"]).id
        data["business_id"] = get_or_create(
            session, Business, name=data["business_name"]
        ).id
        data["branch_id"] = get_or_create(session, Branch, name=data["branch_name"]).id
        data["state_id"] = get_or_create(
            session,
            State,
            name=data["state_name"],
            defaults={"code": data["state_code"]},
        ).id
        data["city_id"] = get_or_create(session, City, name=data["city_name"]).id
        # Note: Place relationship is many-to-many and handled separately if needed.
        # This implementation assumes a simple creation for demonstration.

    # Step 2: Use bulk_insert_mappings for high performance.
    # We only map the columns of the PointOfSale table itself.
    mappings = [
        {
            "code_and_flag": d["code_and_flag"],
            "flag_id": d["flag_id"],
            "business_id": d["business_id"],
            "branch_id": d["branch_id"],
            "state_id": d["state_id"],
            "city_id": d["city_id"],
        }
        for d in pos_data
    ]

    try:
        session.bulk_insert_mappings(PointOfSale, mappings)
        session.commit()
        logger.info(f"Successfully bulk inserted {len(pos_data)} points of sale.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error during bulk insert of points of sale: {e}")
        raise DatabaseException("Failed to bulk insert points of sale.") from e


# --- Article and Price ---


def bulk_insert_prices(session: Session, price_data: List[Dict[str, Any]]):
    """
    Performs a bulk insert of Price records.

    This function is optimized for the 20,000+ price records you receive.

    Example `price_data` entry:
    {
        "amount": 1500, # Price in cents to avoid floating point issues
        "timestamp_str": "20231027100000", # YYYYMMDDHHMMSS
        "article_code_value": "ART456",
        "point_of_sale_code_and_flag": "XYZ-123"
    }

    Args:
        session: The SQLAlchemy session object.
        price_data: A list of dictionaries with price data.
    """
    if not price_data:
        return

    logger.info(f"Starting bulk insert for {len(price_data)} prices.")

    # Step 1: Pre-fetch necessary IDs to avoid querying in a loop.
    # This is a major performance optimization.

    # Get all unique article codes and PoS codes from the input data
    article_codes = {d["article_code_value"] for d in price_data}
    pos_codes = {d["point_of_sale_code_and_flag"] for d in price_data}

    # Fetch corresponding objects from the DB in one go
    article_code_map = {
        ac.value: ac.id
        for ac in session.execute(
            select(ArticleCode).where(ArticleCode.value.in_(article_codes))
        ).scalars()
    }
    pos_map = {
        pos.code_and_flag: pos.id
        for pos in session.execute(
            select(PointOfSale).where(PointOfSale.code_and_flag.in_(pos_codes))
        ).scalars()
    }

    # Step 2: Prepare the data for bulk insertion.
    price_mappings = []
    for data in price_data:
        # Get or create timestamp
        timestamp = get_or_create(
            session, Timestamp, **Timestamp.from_string(data["timestamp_str"]).__dict__
        )

        # Get or create article code
        article_code_id = article_code_map.get(data["article_code_value"])
        if not article_code_id:
            article_code = get_or_create(
                session, ArticleCode, value=data["article_code_value"]
            )
            article_code_id = article_code.id
            article_code_map[data["article_code_value"]] = article_code_id

        pos_id = pos_map.get(data["point_of_sale_code_and_flag"])
        if not pos_id:
            # This case should be rare if PoS data is inserted first.
            logger.warning(
                f"Point of sale '{data['point_of_sale_code_and_flag']}' not found for price entry. Skipping."
            )
            continue

        price_mappings.append(
            {
                "amount": data["amount"],
                "timestamp_id": timestamp.id,
                "article_code_id": article_code_id,
                "point_of_sale_id": pos_id,
            }
        )

    # Step 3: Perform the bulk insert.
    try:
        if price_mappings:
            session.bulk_insert_mappings(Price, price_mappings)
            session.commit()
            logger.info(f"Successfully bulk inserted {len(price_mappings)} prices.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error during bulk insert of prices: {e}")
        raise DatabaseException("Failed to bulk insert prices.") from e


# --- Standard Read/Update/Delete Functions ---


def get_point_of_sale_by_id(session: Session, pos_id: int) -> PointOfSale | None:
    """Retrieves a PointOfSale by its primary key."""
    return session.get(PointOfSale, pos_id)


def update_point_of_sale_address(
    session: Session, pos_id: int, new_address: str
) -> PointOfSale | None:
    """Updates the address for a point of sale."""
    pos = get_point_of_sale_by_id(session, pos_id)
    if pos:
        # Assuming a simple update. For many-to-many 'places', logic would be more complex.
        place = get_or_create(session, Place, address=new_address)
        if place not in pos.places:
            pos.places.append(place)
        session.commit()
        session.refresh(pos)
        logger.info(f"Updated address for PoS ID {pos_id}.")
        return pos
    logger.warning(f"Point of Sale with ID {pos_id} not found for update.")
    return None


def delete_point_of_sale(session: Session, pos_id: int) -> bool:
    """Deletes a PointOfSale and its related prices."""
    pos = get_point_of_sale_by_id(session, pos_id)
    if pos:
        session.delete(pos)
        session.commit()
        logger.info(f"Deleted PoS with ID {pos_id}.")
        return True
    logger.warning(f"Point of Sale with ID {pos_id} not found for deletion.")
    return False


# --- Example Usage ---


def main():
    """Main function to demonstrate CRUD operations."""
    logger.info("--- Starting CRUD Demonstration ---")
    create_database()
    db_session = SessionLocal()

    try:
        # --- 1. Bulk Insert Points of Sale ---
        points_of_sale_to_add = [
            {
                "code_and_flag": "YPF-001",
                "flag_name": "YPF",
                "business_name": "Gas Stations Inc.",
                "branch_name": "Comodoro Rivadavia Central",
                "state_name": "Chubut",
                "state_code": "U",
                "city_name": "Comodoro Rivadavia",
                "address": "San Martin 500",
            },
            {
                "code_and_flag": "SHELL-002",
                "flag_name": "Shell",
                "business_name": "Fuel Corp",
                "branch_name": "Rada Tilly Beach",
                "state_name": "Chubut",
                "state_code": "U",
                "city_name": "Rada Tilly",
                "address": "Moyano 1234",
            },
        ]
        bulk_insert_points_of_sale(db_session, points_of_sale_to_add)

        # --- 2. Bulk Insert Prices ---
        now_str = datetime.now().strftime("%Y%m%d%H%M%S")
        prices_to_add = [
            {
                "amount": 95000,
                "timestamp_str": now_str,
                "article_code_value": "SUPER",
                "point_of_sale_code_and_flag": "YPF-001",
            },
            {
                "amount": 96500,
                "timestamp_str": now_str,
                "article_code_value": "INFINIA",
                "point_of_sale_code_and_flag": "YPF-001",
            },
            {
                "amount": 95500,
                "timestamp_str": now_str,
                "article_code_value": "VPOWER",
                "point_of_sale_code_and_flag": "SHELL-002",
            },
            # Add a large number of prices to simulate the real scenario
            *(
                [
                    {
                        "amount": 105000,
                        "timestamp_str": now_str,
                        "article_code_value": f"DIESEL-{i}",
                        "point_of_sale_code_and_flag": "YPF-001",
                    }
                    for i in range(5000)
                ]
            ),
            *(
                [
                    {
                        "amount": 106000,
                        "timestamp_str": now_str,
                        "article_code_value": f"DIESEL-{i}",
                        "point_of_sale_code_and_flag": "SHELL-002",
                    }
                    for i in range(5000)
                ]
            ),
        ]
        bulk_insert_prices(db_session, prices_to_add)

        # --- 3. Read Operation ---
        logger.info("--- Reading Data ---")
        retrieved_pos = db_session.execute(
            select(PointOfSale).where(PointOfSale.code_and_flag == "YPF-001")
        ).scalar_one_or_none()
        if retrieved_pos:
            logger.info(
                f"Found PoS: {retrieved_pos.code_and_flag}, City: {retrieved_pos.city.name}"
            )
            # Note: Accessing prices will trigger a query because of WriteOnlyMapped
            price_count = (
                db_session.query(Price)
                .filter(Price.point_of_sale_id == retrieved_pos.id)
                .count()
            )
            logger.info(f"It has {price_count} associated price records.")

        # --- 4. Update Operation ---
        logger.info("--- Updating Data ---")
        update_point_of_sale_address(
            db_session, pos_id=retrieved_pos.id, new_address="Rivadavia 100"
        )
        db_session.refresh(retrieved_pos)  # Refresh to see changes
        logger.info(f"New address: {retrieved_pos.places[0].address}")

        # --- 5. Delete Operation ---
        logger.info("--- Deleting Data ---")
        shell_pos = db_session.execute(
            select(PointOfSale).where(PointOfSale.code_and_flag == "SHELL-002")
        ).scalar_one_or_none()
        if shell_pos:
            delete_point_of_sale(db_session, pos_id=shell_pos.id)
            # Verify deletion
            deleted_count = (
                db_session.query(PointOfSale)
                .filter(PointOfSale.id == shell_pos.id)
                .count()
            )
            logger.info(
                f"PoS count for ID {shell_pos.id} after deletion: {deleted_count}"
            )
            price_count_after_delete = (
                db_session.query(Price)
                .filter(Price.point_of_sale_id == shell_pos.id)
                .count()
            )
            logger.info(
                f"Price count for PoS ID {shell_pos.id} after deletion: {price_count_after_delete}"
            )

    except DatabaseException as e:
        logger.error(f"A database operation failed: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        db_session.close()
        logger.info("--- Demonstration Finished ---")

import logging
from typing import List, Optional, Dict, Any, Tuple
from contextlib import contextmanager
from datetime import datetime
from sqlalchemy import create_engine, select, update, delete, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError

# Import the models from the provided schema
from database import (
    Base, Timestamp, State, City, Flag, Business, Branch,
    Place, PointOfSale, ArticleCode, ArticleBrand,
    ArticleDescription, ArticlePackage, ArticleCard, Price,
    DatabaseException
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseCRUD:
    """CRUD operations for the fuel prices database."""

    def __init__(self, db_url: str):
        """
        Initialize the database connection.

        Args:
            db_url: The database connection URL.
        """
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_db(self):
        """Context manager for database sessions."""
        db = self.SessionLocal()
        try:
            yield db
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Database error: {str(e)}")
            raise DatabaseException(f"Database operation failed: {str(e)}")
        finally:
            db.close()

    # Timestamp CRUD operations
    def create_timestamp(self, dt: datetime) -> Timestamp:
        """
        Create a new timestamp record.

        Args:
            dt: A datetime object.

        Returns:
            The created Timestamp object.
        """
        with self.get_db() as db:
            # Check if timestamp already exists
            existing = db.execute(
                select(Timestamp).where(
                    Timestamp.year == dt.year,
                    Timestamp.month == dt.month,
                    Timestamp.day == dt.day,
                    Timestamp.hour == dt.hour,
                    Timestamp.minute == dt.minute,
                    Timestamp.second == dt.second
                )
            ).scalar_one_or_none()

            if existing:
                return existing

            timestamp = Timestamp.from_datetime(dt)
            db.add(timestamp)
            db.flush()  # Flush to get the ID
            return timestamp

    def get_timestamp_by_id(self, timestamp_id: int) -> Optional[Timestamp]:
        """
        Get a timestamp by its ID.

        Args:
            timestamp_id: The ID of the timestamp.

        Returns:
            The Timestamp object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Timestamp, timestamp_id)

    # State CRUD operations
    def create_state(self, code: str, name: str) -> State:
        """
        Create a new state record.

        Args:
            code: The state code.
            name: The state name.

        Returns:
            The created State object.
        """
        with self.get_db() as db:
            # Check if state already exists
            existing = db.execute(
                select(State).where(State.code == code)
            ).scalar_one_or_none()

            if existing:
                return existing

            state = State(code=code, name=name)
            db.add(state)
            db.flush()  # Flush to get the ID
            return state

    def get_state_by_id(self, state_id: int) -> Optional[State]:
        """
        Get a state by its ID.

        Args:
            state_id: The ID of the state.

        Returns:
            The State object or None if not found.
        """
        with self.get_db() as db:
            return db.get(State, state_id)

    def get_state_by_code(self, code: str) -> Optional[State]:
        """
        Get a state by its code.

        Args:
            code: The state code.

        Returns:
            The State object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(State).where(State.code == code)
            ).scalar_one_or_none()

    # City CRUD operations
    def create_city(self, name: str) -> City:
        """
        Create a new city record.

        Args:
            name: The city name.

        Returns:
            The created City object.
        """
        with self.get_db() as db:
            # Check if city already exists
            existing = db.execute(
                select(City).where(City.name == name)
            ).scalar_one_or_none()

            if existing:
                return existing

            city = City(name=name)
            db.add(city)
            db.flush()  # Flush to get the ID
            return city

    def get_city_by_id(self, city_id: int) -> Optional[City]:
        """
        Get a city by its ID.

        Args:
            city_id: The ID of the city.

        Returns:
            The City object or None if not found.
        """
        with self.get_db() as db:
            return db.get(City, city_id)

    def get_city_by_name(self, name: str) -> Optional[City]:
        """
        Get a city by its name.

        Args:
            name: The city name.

        Returns:
            The City object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(City).where(City.name == name)
            ).scalar_one_or_none()

    # Flag CRUD operations
    def create_flag(self, name: str) -> Flag:
        """
        Create a new flag record.

        Args:
            name: The flag name.

        Returns:
            The created Flag object.
        """
        with self.get_db() as db:
            # Check if flag already exists
            existing = db.execute(
                select(Flag).where(Flag.name == name)
            ).scalar_one_or_none()

            if existing:
                return existing

            flag = Flag(name=name)
            db.add(flag)
            db.flush()  # Flush to get the ID
            return flag

    def get_flag_by_id(self, flag_id: int) -> Optional[Flag]:
        """
        Get a flag by its ID.

        Args:
            flag_id: The ID of the flag.

        Returns:
            The Flag object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Flag, flag_id)

    def get_flag_by_name(self, name: str) -> Optional[Flag]:
        """
        Get a flag by its name.

        Args:
            name: The flag name.

        Returns:
            The Flag object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(Flag).where(Flag.name == name)
            ).scalar_one_or_none()

    # Business CRUD operations
    def create_business(self, name: str) -> Business:
        """
        Create a new business record.

        Args:
            name: The business name.

        Returns:
            The created Business object.
        """
        with self.get_db() as db:
            # Check if business already exists
            existing = db.execute(
                select(Business).where(Business.name == name)
            ).scalar_one_or_none()

            if existing:
                return existing

            business = Business(name=name)
            db.add(business)
            db.flush()  # Flush to get the ID
            return business

    def get_business_by_id(self, business_id: int) -> Optional[Business]:
        """
        Get a business by its ID.

        Args:
            business_id: The ID of the business.

        Returns:
            The Business object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Business, business_id)

    def get_business_by_name(self, name: str) -> Optional[Business]:
        """
        Get a business by its name.

        Args:
            name: The business name.

        Returns:
            The Business object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(Business).where(Business.name == name)
            ).scalar_one_or_none()

    # Branch CRUD operations
    def create_branch(self, name: str) -> Branch:
        """
        Create a new branch record.

        Args:
            name: The branch name.

        Returns:
            The created Branch object.
        """
        with self.get_db() as db:
            branch = Branch(name=name)
            db.add(branch)
            db.flush()  # Flush to get the ID
            return branch

    def get_branch_by_id(self, branch_id: int) -> Optional[Branch]:
        """
        Get a branch by its ID.

        Args:
            branch_id: The ID of the branch.

        Returns:
            The Branch object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Branch, branch_id)

    # Place CRUD operations
    def create_place(self, address: str) -> Place:
        """
        Create a new place record.

        Args:
            address: The place address.

        Returns:
            The created Place object.
        """
        with self.get_db() as db:
            # Check if place already exists
            existing = db.execute(
                select(Place).where(Place.address == address)
            ).scalar_one_or_none()

            if existing:
                return existing

            place = Place(address=address)
            db.add(place)
            db.flush()  # Flush to get the ID
            return place

    def get_place_by_id(self, place_id: int) -> Optional[Place]:
        """
        Get a place by its ID.

        Args:
            place_id: The ID of the place.

        Returns:
            The Place object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Place, place_id)

    # PointOfSale CRUD operations
    def create_point_of_sale(
        self,
        code_and_flag: str,
        flag_id: int,
        business_id: int,
        branch_id: int,
        state_id: int,
        city_id: int,
        places: List[Place] = None
    ) -> PointOfSale:
        """
        Create a new point of sale record.

        Args:
            code_and_flag: The code and flag identifier.
            flag_id: The ID of the flag.
            business_id: The ID of the business.
            branch_id: The ID of the branch.
            state_id: The ID of the state.
            city_id: The ID of the city.
            places: List of Place objects associated with this point of sale.

        Returns:
            The created PointOfSale object.
        """
        with self.get_db() as db:
            # Check if point of sale already exists
            existing = db.execute(
                select(PointOfSale).where(PointOfSale.code_and_flag == code_and_flag)
            ).scalar_one_or_none()

            if existing:
                return existing

            point_of_sale = PointOfSale(
                code_and_flag=code_and_flag,
                flag_id=flag_id,
                business_id=business_id,
                branch_id=branch_id,
                state_id=state_id,
                city_id=city_id
            )

            if places:
                point_of_sale.places = places

            db.add(point_of_sale)
            db.flush()  # Flush to get the ID
            return point_of_sale

    def get_point_of_sale_by_id(self, pos_id: int) -> Optional[PointOfSale]:
        """
        Get a point of sale by its ID.

        Args:
            pos_id: The ID of the point of sale.

        Returns:
            The PointOfSale object or None if not found.
        """
        with self.get_db() as db:
            return db.get(PointOfSale, pos_id)

    def get_point_of_sale_by_code(self, code: str) -> Optional[PointOfSale]:
        """
        Get a point of sale by its code and flag.

        Args:
            code: The code and flag identifier.

        Returns:
            The PointOfSale object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(PointOfSale).where(PointOfSale.code_and_flag == code)
            ).scalar_one_or_none()

    # ArticleCode CRUD operations
    def create_article_code(self, value: str) -> ArticleCode:
        """
        Create a new article code record.

        Args:
            value: The article code value.

        Returns:
            The created ArticleCode object.
        """
        with self.get_db() as db:
            # Check if article code already exists
            existing = db.execute(
                select(ArticleCode).where(ArticleCode.value == value)
            ).scalar_one_or_none()

            if existing:
                return existing

            article_code = ArticleCode(value=value)
            db.add(article_code)
            db.flush()  # Flush to get the ID
            return article_code

    def get_article_code_by_id(self, code_id: int) -> Optional[ArticleCode]:
        """
        Get an article code by its ID.

        Args:
            code_id: The ID of the article code.

        Returns:
            The ArticleCode object or None if not found.
        """
        with self.get_db() as db:
            return db.get(ArticleCode, code_id)

    def get_article_code_by_value(self, value: str) -> Optional[ArticleCode]:
        """
        Get an article code by its value.

        Args:
            value: The article code value.

        Returns:
            The ArticleCode object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(ArticleCode).where(ArticleCode.value == value)
            ).scalar_one_or_none()

    # ArticleBrand CRUD operations
    def create_article_brand(self, value: str) -> ArticleBrand:
        """
        Create a new article brand record.

        Args:
            value: The article brand value.

        Returns:
            The created ArticleBrand object.
        """
        with self.get_db() as db:
            # Check if article brand already exists
            existing = db.execute(
                select(ArticleBrand).where(ArticleBrand.value == value)
            ).scalar_one_or_none()

            if existing:
                return existing

            article_brand = ArticleBrand(value=value)
            db.add(article_brand)
            db.flush()  # Flush to get the ID
            return article_brand

    def get_article_brand_by_id(self, brand_id: int) -> Optional[ArticleBrand]:
        """
        Get an article brand by its ID.

        Args:
            brand_id: The ID of the article brand.

        Returns:
            The ArticleBrand object or None if not found.
        """
        with self.get_db() as db:
            return db.get(ArticleBrand, brand_id)

    def get_article_brand_by_value(self, value: str) -> Optional[ArticleBrand]:
        """
        Get an article brand by its value.

        Args:
            value: The article brand value.

        Returns:
            The ArticleBrand object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(ArticleBrand).where(ArticleBrand.value == value)
            ).scalar_one_or_none()

    # ArticleDescription CRUD operations
    def create_article_description(self, value: str) -> ArticleDescription:
        """
        Create a new article description record.

        Args:
            value: The article description value.

        Returns:
            The created ArticleDescription object.
        """
        with self.get_db() as db:
            # Check if article description already exists
            existing = db.execute(
                select(ArticleDescription).where(ArticleDescription.value == value)
            ).scalar_one_or_none()

            if existing:
                return existing

            article_description = ArticleDescription(value=value)
            db.add(article_description)
            db.flush()  # Flush to get the ID
            return article_description

    def get_article_description_by_id(self, desc_id: int) -> Optional[ArticleDescription]:
        """
        Get an article description by its ID.

        Args:
            desc_id: The ID of the article description.

        Returns:
            The ArticleDescription object or None if not found.
        """
        with self.get_db() as db:
            return db.get(ArticleDescription, desc_id)

    def get_article_description_by_value(self, value: str) -> Optional[ArticleDescription]:
        """
        Get an article description by its value.

        Args:
            value: The article description value.

        Returns:
            The ArticleDescription object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(ArticleDescription).where(ArticleDescription.value == value)
            ).scalar_one_or_none()

    # ArticlePackage CRUD operations
    def create_article_package(self, value: str) -> ArticlePackage:
        """
        Create a new article package record.

        Args:
            value: The article package value.

        Returns:
            The created ArticlePackage object.
        """
        with self.get_db() as db:
            # Check if article package already exists
            existing = db.execute(
                select(ArticlePackage).where(ArticlePackage.value == value)
            ).scalar_one_or_none()

            if existing:
                return existing

            article_package = ArticlePackage(value=value)
            db.add(article_package)
            db.flush()  # Flush to get the ID
            return article_package

    def get_article_package_by_id(self, package_id: int) -> Optional[ArticlePackage]:
        """
        Get an article package by its ID.

        Args:
            package_id: The ID of the article package.

        Returns:
            The ArticlePackage object or None if not found.
        """
        with self.get_db() as db:
            return db.get(ArticlePackage, package_id)

    def get_article_package_by_value(self, value: str) -> Optional[ArticlePackage]:
        """
        Get an article package by its value.

        Args:
            value: The article package value.

        Returns:
            The ArticlePackage object or None if not found.
        """
        with self.get_db() as db:
            return db.execute(
                select(ArticlePackage).where(ArticlePackage.value == value)
            ).scalar_one_or_none()

    # ArticleCard CRUD operations
    def create_article_card(
        self,
        brand_id: int,
        description_id: int,
        package_id: int,
        code_id: int
    ) -> ArticleCard:
        """
        Create a new article card record.

        Args:
            brand_id: The ID of the article brand.
            description_id: The ID of the article description.
            package_id: The ID of the article package.
            code_id: The ID of the article code.

        Returns:
            The created ArticleCard object.
        """
        with self.get_db() as db:
            # Check if article card already exists
            existing = db.execute(
                select(ArticleCard).where(
                    ArticleCard.brand_id == brand_id,
                    ArticleCard.description_id == description_id,
                    ArticleCard.package_id == package_id,
                    ArticleCard.code_id == code_id
                )
            ).scalar_one_or_none()

            if existing:
                return existing

            article_card = ArticleCard(
                brand_id=brand_id,
                description_id=description_id,
                package_id=package_id,
                code_id=code_id
            )
            db.add(article_card)
            db.flush()  # Flush to get the ID
            return article_card

    def get_article_card_by_id(self, card_id: int) -> Optional[ArticleCard]:
        """
        Get an article card by its ID.

        Args:
            card_id: The ID of the article card.

        Returns:
            The ArticleCard object or None if not found.
        """
        with self.get_db() as db:
            return db.get(ArticleCard, card_id)

    # Price CRUD operations
    def create_price(
        self,
        amount: int,
        timestamp_id: int,
        article_code_id: int,
        point_of_sale_id: int
    ) -> Price:
        """
        Create a new price record.

        Args:
            amount: The price amount.
            timestamp_id: The ID of the timestamp.
            article_code_id: The ID of the article code.
            point_of_sale_id: The ID of the point of sale.

        Returns:
            The created Price object.
        """
        with self.get_db() as db:
            # Check if price already exists
            existing = db.execute(
                select(Price).where(
                    Price.timestamp_id == timestamp_id,
                    Price.article_code_id == article_code_id,
                    Price.point_of_sale_id == point_of_sale_id
                )
            ).scalar_one_or_none()

            if existing:
                # Update existing price
                existing.amount = amount
                db.flush()  # Flush to update the record
                return existing

            price = Price(
                amount=amount,
                timestamp_id=timestamp_id,
                article_code_id=article_code_id,
                point_of_sale_id=point_of_sale_id
            )
            db.add(price)
            db.flush()  # Flush to get the ID
            return price

    def get_price_by_id(self, price_id: int) -> Optional[Price]:
        """
        Get a price by its ID.

        Args:
            price_id: The ID of the price.

        Returns:
            The Price object or None if not found.
        """
        with self.get_db() as db:
            return db.get(Price, price_id)

    # Bulk operations for weekly updates
    def bulk_create_points_of_sale(self, points_of_sale_data: List[Dict[str, Any]]) -> List[PointOfSale]:
        """
        Bulk create points of sale.

        Args:
            points_of_sale_data: List of dictionaries containing point of sale data.

        Returns:
            List of created or existing PointOfSale objects.
        """
        with self.get_db() as db:
            results = []

            for pos_data in points_of_sale_data:
                try:
                    # Get or create related entities
                    state = self.create_state(pos_data['state_code'], pos_data['state_name'])
                    city = self.create_city(pos_data['city_name'])
                    flag = self.create_flag(pos_data['flag_name'])
                    business = self.create_business(pos_data['business_name'])
                    branch = self.create_branch(pos_data['branch_name'])

                    # Create places if any
                    places = []
                    if 'addresses' in pos_data:
                        for address in pos_data['addresses']:
                            place = self.create_place(address)
                            places.append(place)

                    # Create point of sale
                    pos = self.create_point_of_sale(
                        code_and_flag=pos_data['code_and_flag'],
                        flag_id=flag.id,
                        business_id=business.id,
                        branch_id=branch.id,
                        state_id=state.id,
                        city_id=city.id,
                        places=places
                    )
                    results.append(pos)
                except Exception as e:
                    logger.error(f"Error creating point of sale {pos_data.get('code_and_flag', 'unknown')}: {str(e)}")
                    continue

            return results

    def bulk_create_prices(self, prices_data: List[Dict[str, Any]]) -> List[Price]:
        """
        Bulk create prices.

        Args:
            prices_data: List of dictionaries containing price data.

        Returns:
            List of created or updated Price objects.
        """
        with self.get_db() as db:
            results = []
            processed_count = 0
            batch_size = 1000  # Process in batches to avoid memory issues

            for i in range(0, len(prices_data), batch_size):
                batch = prices_data[i:i+batch_size]

                for price_data in batch:
                    try:
                        # Get or create timestamp
                        timestamp = self.create_timestamp(price_data['timestamp'])

                        # Get or create article code
                        article_code = self.create_article_code(price_data['article_code'])

                        # Get point of sale
                        pos = self.get_point_of_sale_by_code(price_data['pos_code'])
                        if not pos:
                            logger.warning(f"Point of sale with code {price_data['pos_code']} not found")
                            continue

                        # Create price
                        price = self.create_price(
                            amount=price_data['amount'],
                            timestamp_id=timestamp.id,
                            article_code_id=article_code.id,
                            point_of_sale_id=pos.id
                        )
                        results.append(price)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Error creating price for article {price_data.get('article_code', 'unknown')} at POS {price_data.get('pos_code', 'unknown')}: {str(e)}")
                        continue

                # Commit after each batch
                db.commit()
                logger.info(f"Processed {processed_count} price records so far...")

            return results

    def bulk_create_articles(self, articles_data: List[Dict[str, Any]]) -> List[ArticleCard]:
        """
        Bulk create articles.

        Args:
            articles_data: List of dictionaries containing article data.

        Returns:
            List of created or existing ArticleCard objects.
        """
        with self.get_db() as db:
            results = []

            for article_data in articles_data:
                try:
                    # Get or create article components
                    brand = self.create_article_brand(article_data['brand'])
                    description = self.create_article_description(article_data['description'])
                    package = self.create_article_package(article_data['package'])
                    code = self.create_article_code(article_data['code'])

                    # Create article card
                    article_card = self.create_article_card(
                        brand_id=brand.id,
                        description_id=description.id,
                        package_id=package.id,
                        code_id=code.id
                    )
                    results.append(article_card)
                except Exception as e:
                    logger.error(f"Error creating article {article_data.get('code', 'unknown')}: {str(e)}")
                    continue

            return results

    # Query operations
    def get_prices_by_point_of_sale(self, pos_id: int, limit: int = 100) -> List[Price]:
        """
        Get prices for a specific point of sale.

        Args:
            pos_id: The ID of the point of sale.
            limit: Maximum number of records to return.

        Returns:
            List of Price objects.
        """
        with self.get_db() as db:
            return db.execute(
                select(Price)
                .where(Price.point_of_sale_id == pos_id)
                .order_by(Price.timestamp_id.desc())
                .limit(limit)
            ).scalars().all()

    def get_prices_by_article(self, article_code_id: int, limit: int = 100) -> List[Price]:
        """
        Get prices for a specific article.

        Args:
            article_code_id: The ID of the article code.
            limit: Maximum number of records to return.

        Returns:
            List of Price objects.
        """
        with self.get_db() as db:
            return db.execute(
                select(Price)
                .where(Price.article_code_id == article_code_id)
                .order_by(Price.timestamp_id.desc())
                .limit(limit)
            ).scalars().all()

    def get_prices_by_date_range(self, start_date: datetime, end_date: datetime, limit: int = 1000) -> List[Price]:
        """
        Get prices within a date range.

        Args:
            start_date: Start of the date range.
            end_date: End of the date range.
            limit: Maximum number of records to return.

        Returns:
            List of Price objects.
        """
        with self.get_db() as db:
            # Convert dates to timestamps
            start_timestamp = Timestamp.from_datetime(start_date)
            end_timestamp = Timestamp.from_datetime(end_date)

            return db.execute(
                select(Price)
                .join(Price.timestamp)
                .where(
                    Timestamp.year >= start_timestamp.year,
                    Timestamp.month >= start_timestamp.month,
                    Timestamp.day >= start_timestamp.day,
                    Timestamp.year <= end_timestamp.year,
                    Timestamp.month <= end_timestamp.month,
                    Timestamp.day <= end_timestamp.day
                )
                .order_by(Price.timestamp_id.desc())
                .limit(limit)
            ).scalars().all()

    def get_average_price_by_article(self, article_code_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Optional[float]:
        """
        Get the average price for an article, optionally within a date range.

        Args:
            article_code_id: The ID of the article code.
            start_date: Start of the date range (optional).
            end_date: End of the date range (optional).

        Returns:
            The average price or None if no prices found.
        """
        with self.get_db() as db:
            query = select(func.avg(Price.amount)).where(Price.article_code_id == article_code_id)

            if start_date and end_date:
                start_timestamp = Timestamp.from_datetime(start_date)
                end_timestamp = Timestamp.from_datetime(end_date)

                query = query.join(Price.timestamp).where(
                    Timestamp.year >= start_timestamp.year,
                    Timestamp.month >= start_timestamp.month,
                    Timestamp.day >= start_timestamp.day,
                    Timestamp.year <= end_timestamp.year,
                    Timestamp.month <= end_timestamp.month,
                    Timestamp.day <= end_timestamp.day
                )

            result = db.execute(query).scalar_one_or_none()
            return float(result) if result is not None else None

    def get_price_statistics_by_article(self, article_code_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, float]:
        """
        Get price statistics for an article, optionally within a date range.

        Args:
            article_code_id: The ID of the article code.
            start_date: Start of the date range (optional).
            end_date: End of the date range (optional).

        Returns:
            Dictionary containing price statistics (min, max, avg).
        """
        with self.get_db() as db:
            query = select(
                func.min(Price.amount),
                func.max(Price.amount),
                func.avg(Price.amount)
            ).where(Price.article_code_id == article_code_id)

            if start_date and end_date:
                start_timestamp = Timestamp.from_datetime(start_date)
                end_timestamp = Timestamp.from_datetime(end_date)

                query = query.join(Price.timestamp).where(
                    Timestamp.year >= start_timestamp.year,
                    Timestamp.month >= start_timestamp.month,
                    Timestamp.day >= start_timestamp.day,
                    Timestamp.year <= end_timestamp.year,
                    Timestamp.month <= end_timestamp.month,
                    Timestamp.day <= end_timestamp.day
                )

            result = db.execute(query).one()

            return {
                'min': float(result[0]) if result[0] is not None else None,
                'max': float(result[1]) if result[1] is not None else None,
                'avg': float(result[2]) if result[2] is not None else None
            }

    def get_price_trends_by_article(self, article_code_id: int, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get price trends for an article over a specified number of days.

        Args:
            article_code_id: The ID of the article code.
            days: Number of days to look back.

        Returns:
            List of dictionaries containing date and average price for each day.
        """
        with self.get_db() as db:
            # Calculate the start date
            end_date = datetime.now()
            start_date = datetime(end_date.year, end_date.month, end_date.day - days)

            # Get all prices within the date range
            prices = db.execute(
                select(Price, Timestamp)
                .join(Price.timestamp)
                .where(
                    Price.article_code_id == article_code_id,
                    Timestamp.year >= start_date.year,
                    Timestamp.month >= start_date.month,
                    Timestamp.day >= start_date.day,
                    Timestamp.year <= end_date.year,
                    Timestamp.month <= end_date.month,
                    Timestamp.day <= end_date.day
                )
                .order_by(Timestamp.year, Timestamp.month, Timestamp.day, Timestamp.hour)
            ).all()

            # Group by date and calculate average price
            daily_prices = {}
            for price, timestamp in prices:
                date_key = (timestamp.year, timestamp.month, timestamp.day)
                if date_key not in daily_prices:
                    daily_prices[date_key] = []
                daily_prices[date_key].append(price.amount)

            # Calculate average for each day
            trends = []
            for (year, month, day), amounts in daily_prices.items():
                trends.append({
                    'date': datetime(year, month, day),
                    'avg_price': sum(amounts) / len(amounts)
                })

            # Sort by date
            trends.sort(key=lambda x: x['date'])

            return trends

    def get_lowest_prices_by_article(self, article_code_id: int, limit: int = 10) -> List[Tuple[Price, PointOfSale]]:
        """
        Get the lowest prices for an article across all points of sale.

        Args:
            article_code_id: The ID of the article code.
            limit: Maximum number of records to return.

        Returns:
            List of tuples containing Price and PointOfSale objects.
        """
        with self.get_db() as db:
            return db.execute(
                select(Price, PointOfSale)
                .join(Price.point_of_sale)
                .where(Price.article_code_id == article_code_id)
                .order_by(Price.amount.asc())
                .limit(limit)
            ).all()

    def get_highest_prices_by_article(self, article_code_id: int, limit: int = 10) -> List[Tuple[Price, PointOfSale]]:
        """
        Get the highest prices for an article across all points of sale.

        Args:
            article_code_id: The ID of the article code.
            limit: Maximum number of records to return.

        Returns:
            List of tuples containing Price and PointOfSale objects.
        """
        with self.get_db() as db:
            return db.execute(
                select(Price, PointOfSale)
                .join(Price.point_of_sale)
                .where(Price.article_code_id == article_code_id)
                .order_by(Price.amount.desc())
                .limit(limit)
            ).all()

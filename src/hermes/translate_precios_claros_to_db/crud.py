import logging
from datetime import datetime
from typing import List, Dict, Any, TypeAlias

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

# Import the models from your database definition file
from hermes.domain.database import (
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
    ArticleBrand,
    ArticleDescription,
    ArticlePackage,
    ArticleCard,
    Price,
    DatabaseException,
)


AnyModel: TypeAlias = (
    Timestamp | State | City | Flag | Business | Branch | Place | PointOfSale |
    ArticleCode | ArticleBrand | ArticleDescription | ArticlePackage | ArticleCard | Price
)



class DatabaseCRUD:
    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def session(self) -> Session:
        return self._session

    def get_timestamp(
        self,
        row: dict[str, Any]
    ) -> Timestamp | None:
        """
        Queries the database for a Timestamp record by its specific code.

        Args:
            timestamp_target: The tiemstamp to
            (e.g., '20250730180000').

        Returns:
            The Timestamp object if found, otherwise None.
            Raises an exception if multiple Timestamp are found
            which would indicate a data integrity issue.
        """
        timestamp_string = row.get("timestamp", False)
        if not timestamp_string:
            message = f"no timestamp here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            timestamp_target = Timestamp.from_string(timestamp_string)
            # Construct the query using a select statement
            query = select(Timestamp).where(
                Timestamp.year == timestamp_target.year,
                Timestamp.month == timestamp_target.month,
                Timestamp.day == timestamp_target.day,
                Timestamp.hour == timestamp_target.hour,
                Timestamp.minute == timestamp_target.minute,
                Timestamp.second == timestamp_target.second,
            )
            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying timestamp '{timestamp_target}': {panic}"
            raise DatabaseException(message) from panic


    def get_state(
        self,
        row: dict[str, Any]
    ) -> State | None:
        """
        Queries the database for a State record by its specific code.

        Args:
            row: a dict[str, Any] with a state_code key and the respective value

        Returns:
            The State object if found, otherwise None.
            Raises an exception if multiple states are found with the same code,
            which would indicate a data integrity issue.
        """
        state_code = row.get("state_code", False)
        if not state_code:
            message = f"no state_code here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(State).where(State.code == state_code)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for state code '{state_code}': {panic}"
            raise DatabaseException(message) from panic


    def get_city(
        self,
        row: dict[str, Any]
    ) -> City | None:
        """
        Queries the database for a City record by its specific code.

        Args:
            row: a dict[str, Any] with a key city_code and the respective value
            is code of the city to retrieve

        Returns:
            The City object if found, otherwise None.
            Raises an exception if multiple cities are found with the same code,
            which would indicate a data integrity issue.
        """
        city_code = row.get("city_code", False)
        if not city_code:
            message = f"no city_code here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(City).where(City.code == city_code)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for city_code '{city_code}': {panic}"
            raise DatabaseException(message) from panic

    def get_flag(
        self,
        row: dict[str, Any]
    ) -> Flag | None:
        """
        Queries the database for a Flag record by its specific name.

        Args:
            row: a dict[str, Any] with a key flag and the respective value
            is the name of the Flag to retrieve

        Returns:
            The Flag object if found, otherwise None.
            Raises an exception if multiple flags are found with the same name,
            which would indicate a data integrity issue.
        """
        flag = row.get("flag", False)
        if not flag:
            message = f"no flag here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(Flag).where(Flag.name == flag)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for flag '{flag}': {panic}"
            raise DatabaseException(message) from panic

    def get_business(
        self,
        row: dict[str, Any]
    ) -> Business | None:
        """
        Queries the database for a Business record by its specific name.

        Args:
            row: a dict[str, Any] with a key business and the respective value
            is the name of the Business to retrieve

        Returns:
            The Business object if found, otherwise None.
            Raises an exception if multiple businesses are found with the same name,
            which would indicate a data integrity issue.
        """
        business = row.get("business", False)
        if not business:
            message = f"no business here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(Business).where(Business.name == bussiness)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for business '{business}': {panic}"
            raise DatabaseException(message) from panic

    def get_branch(
        self,
        row: dict[str, Any]
    ) -> Branch | None:
        """
        Queries the database for a Branch record by its specific name.

        Args:
            row: a dict[str, Any] with a key branch and the respective value
            is the name of the Branch to retrieve

        Returns:
            The Branch object if found, otherwise None.
            Raises an exception if multiple branches are found with the same name,
            which would indicate a data integrity issue.
        """
        branch = row.get("branch", False)
        if not branch:
            message = f"no branch here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(Branch).where(Branch.name == branch)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for branch '{branch}': {panic}"
            raise DatabaseException(message) from panic

    def get_place(
        self,
        row: dict[str, Any]
    ) -> Branch | None:
        """
        Queries the database for a Place record by its specific address.

        Args:
            row: a dict[str, Any] with a key address and the respective value
            is the address of the Place to retrieve

        Returns:
            The Place object if found, otherwise None.
            Raises an exception if multiple Places are found with the same address,
            which would indicate a data integrity issue.
        """
        address = row.get("address", False)
        if not address:
            message = f"no address here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(Place).where(Place.address == address)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for place '{address}': {panic}"
            raise DatabaseException(message) from panic


    def get_point_of_sale(
        self,
        row: dict[str, Any]
    ) -> PointOfSale | None:
        """
        Queries the database for a PointOfSale record by its specific code_and_flag.

        Args:
            row: a dict[str, Any] with a key code_and_flag and the respective value
            is the code_and_flag of the PointOfSale to retrieve

        Returns:
            The PointOfSale object if found, otherwise None.
            Raises an exception if multiple PointOfSale are found with the same code_and_flag,
            which would indicate a data integrity issue.
        """
        code_and_flag = row.get("code_and_flag", False)
        if not code_and_flag:
            message = f"no code_and_flag here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(PointOfSale).where(PointOfSale.code_and_flag == code_and_flag)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for code_and_flag '{code_and_flag}': {panic}"
            raise DatabaseException(message) from panic


    def get_article_code(
        self,
        row: dict[str, Any]
    ) -> ArticleCode | None:
        """
        Queries the database for a ArticleCode record by its specific article_code.

        Args:
            row: a dict[str, Any] with a key article_code and the respective value
            is the article_code of the ArticleCode to retrieve

        Returns:
            The ArticleCode object if found, otherwise None.
            Raises an exception if multiple ArticleCode are found with the same article_code,
            which would indicate a data integrity issue.
        """
        article_code = row.get("article_code", False)
        if not article_code:
            message = f"no article_code here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(ArticleCode).where(ArticleCode.article_code == article_code)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for article_code '{article_code}': {panic}"
            raise DatabaseException(message) from panic

    def get_article_brand(
        self,
        row: dict[str, Any]
    ) -> ArticleBrand | None:
        """
        Queries the database for a ArticleBrand record by its specific article_brand.

        Args:
            row: a dict[str, Any] with a key article_brand and the respective value
            is the article_brand of the ArticleBrand to retrieve

        Returns:
            The ArticleBrand object if found, otherwise None.
            Raises an exception if multiple ArticleBrand are found with the same article_brand,
            which would indicate a data integrity issue.
        """
        article_brand = row.get("article_brand", False)
        if not article_brand:
            message = f"no article_brand here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(ArticleBrand).where(ArticleBrand.article_brand == article_brand)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for article_brand '{article_brand}': {panic}"
            raise DatabaseException(message) from panic


    def get_article_description(
        self,
        row: dict[str, Any]
    ) -> ArticleDescription | None:
        """
        Queries the database for a ArticleDescription record by its specific article_description.

        Args:
            row: a dict[str, Any] with a key article_description and the respective value
            is the article_description of the ArticleDescription to retrieve

        Returns:
            The ArticleDescription object if found, otherwise None.
            Raises an exception if multiple ArticleDescription are found with the same article_description,
            which would indicate a data integrity issue.
        """
        article_description = row.get("article_description", False)
        if not article_description:
            message = f"no article_description here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(ArticleDescription).where(ArticleDescription.article_description == article_description)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for article_description '{article_description}': {panic}"
            raise DatabaseException(message) from panic


    def get_article_package(
        self,
        row: dict[str, Any]
    ) -> ArticlePackage | None:
        """
        Queries the database for a ArticlePackage record by its specific article_package.

        Args:
            row: a dict[str, Any] with a key article_package and the respective value
            is the article_package of the ArticlePackage to retrieve

        Returns:
            The ArticlePackage object if found, otherwise None.
            Raises an exception if multiple ArticlePackage are found with the same article_package,
            which would indicate a data integrity issue.
        """
        article_package = row.get("article_package", False)
        if not article_package:
            message = f"no article_package here {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(ArticlePackage).where(ArticlePackage.article_package == article_package)

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for article_package '{article_package}': {panic}"
            raise DatabaseException(message) from panic

    def get_article_card(
        self,
        row: dict[str, Any]
    ) -> ArticleCard | None:
        """
        Queries the database for a ArticleCard record by its specific fields

        Args:
            row: a dict[str, Any] with a key article_package and the respective value
            is the article_package of the ArticlePackage to retrieve

        Returns:
            The ArticleCard object if found, otherwise None.
            Raises an exception if multiple ArticleCard are found with the same article_package,
            which would indicate a data integrity issue.
        """
        test_article_brand = self.get_article_brand(row)
        test_article_description = self.get_article_description(row)
        test_article_package = self.get_article_package(row)
        test_article_code = self.get_article_code(row)
        article_brand = test_article_brand if test_article_brand else False
        article_description = test_article_description if test_article_description else False
        article_package = test_article_package if test_article_package else False
        article_code = test_article_package if test_article_code else False
        test = article_brand and article_description and article_package and article_code
        if not test:
            message = f"check article_brand, article_description, article_package, article_code {row_to_string(row)}"
            raise DatabaseException(message)
        try:
            # Construct the query using a select statement
            query = select(ArticleCard).where(
                ArticleCard.article_brand_id == article_brand.id,
                ArticleCard.article_description_id == article_description.id,
                ArticleCard.article_package_id == article_package.id,
                ArticleCard.article_code_id == article_code.id
            )

            # Execute the query and get a single scalar result.
            # .scalar_one_or_none() is ideal here because it returns:
            # - The object if exactly one is found.
            # - None if no object is found.
            # - Raises an error if multiple objects are found.
            result = session.execute(query).scalar_one_or_none()
            return result
        except Exception as panic:
            # Catch potential errors, like MultipleResultsFound
            message = f"An error occurred while querying for article_card {row_to_string(row)} {panic}"
            raise DatabaseException(message) from panic


    def insert_timestamps(
        self,
        rows_generator: Callable[[], Generator[dict[str, Any], None, None]]
    ) -> None:
        try:
            bulk = [
                timestamp_row_from_row(row)
                for row in rows_generator() if get_timestamp(row) is None
            ]
            if len(bulk):
                self.session.execute(insert(Timestamp), bulk)
            else:
                logger.info(f"{self.__class__.__name__}.insert_timestamps: empty bulk")
        except Exception as panic:
            self.session.rollback()
            message = f"{self.__class__.__name__}.insert_timestamps stop: {panic}***"
            raise DatabaseException(message) from panic


    def insert_states(
        self,
        records_generator: Generator[dict[str, Any], None, None]
    ) -> None:
        try:
            bulk = [
                state_row_from_row(record)
                for record in states_generator() if get_state(record) is None
            ]
            if len(bulk):
                self.session.execute(insert(State), bulk)
                self.session.commit()
            else:
                logger.info(f"{self.__class__.__name__}.insert_states empty bulk")
        except Exception as panic:
            self.session.rollback()
            message = f"***{self.__class__.__name__}.insert_states {str(panic)}***"
            raise DatabaseException(message)


    def insert_cities(
        self,
        records_generator: Generator[dict[str, Any], None, None]
    ) -> None:
        try:
            bulk = [
                city_row_from_row(record)
                for record in record_generator() if get_city(record) is None
            ]
            if len(bulk):
                self.session.execute(insert(City), bulk)
                self.session.commit()
            else:
                logger.info(f"{self.__class__.__name__}.insert_cities empty bulk")
        except Exception as panic:
            self.session.rollback()
            message = f"***{self.__class__.__name__}.insert_cities {str(panic)}***"
            raise DatabaseException(message)


    def insert_cities(
        self,
        records_generator: Generator[dict[str, Any], None, None]
    ) -> None:
        try:
            bulk = [
                city_row_from_row(record)
                for record in record_generator() if get_city(record) is None
            ]
            if len(bulk):
                self.session.execute(insert(City), bulk)
                self.session.commit()
            else:
                logger.info(f"{self.__class__.__name__}.insert_cities empty bulk")
        except Exception as panic:
            self.session.rollback()
            message = f"***{self.__class__.__name__}.insert_cities {str(panic)}***"
            raise DatabaseException(message)


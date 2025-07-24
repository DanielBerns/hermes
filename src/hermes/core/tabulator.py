class PricesTable:
    def __init__(self, title: str) -> None:
        self._table = defaultdict(
            lambda: defaultdict(
                lambda: defauldict(
                    lambda: defaultdict(lambda: (0, 0, 0))))
            )
        )

    @property
    def table(self) -> defaultdict[str, defaultdict[str, defaultdict[str, tuple[int, int, int]]]]:
        return self._table

    def iterate_city(self) -> Generator[Tuple[City, str], None, None]:
        for city_key, city_flag in self.cities_table.items():
            if not city_flag:
                continue
            city_target = db.all_the_cities.get(city_key, None)
            if city_target is None:
                message = f"{self.__class__.__name__} failure: unknown {city_key}"
                raise QuerySomePricesException(message)
            else:
                yield city_target, city_key

    def iterate_timestamp(self) -> Generator[Timestamp, None, None]:
        for timestamp_key, timestamp_flag in self.timestamps_table.items():
            if not timestamp_flag:
                continue
            timestamp_target = db.all_the_timestamps.get(timestamp_key, None)
            if timestamp_target is None:
                message = f"{self.__class__.__name__} failure: unknown {timestamp_key}"
                raise QuerySomePricesException(message)
            else:
                yield timestamp_target, timestamp_key

    def iterate_article_code(self) -> Generator[ArticleCode, None, None]:
        for article_code_key, article_code_flag in self.article_codes_table.items():
            if not article_code_flag:
                continue
            article_code_target = db.all_the_article_codes.get(article_code_key, None)
            if article_code_target is None:
                message = f"{self.__class__.__name__} failure: unknown {article_code_key}"
                raise QuerySomePricesException(message)
            else:
                yield article_code_target, article_code_key

    def get_table(
        self,
        db: DB,
        cities_table: dict[str, bool],
        article_codes_table: dict[str, bool],
        timestamps_table: dict[str, bool]
    ) -> defaultdict[str, defaultdict[str, defaultdict[str, tuple[int, int, int]]]]:
        table = defaultdict(
            lambda: defaultdict(
                lambda: defauldict(
                    lambda: defaultdict(lambda: (0, 0, 0))))
            )
        )
        for city_target, city_key in self.iterate_city(db):
            for article_code_target, article_code_key in self.iterate_article_code():
                for timestamp_target, timestamp_key in self.iterate_timestamp():
                    for row in db.get_price_summary(
                       article_code_target,
                       city_target,
                       timestamp_target,
                       aggregation_function=geometric_mean_times_100
                    ):
                        table[city_key][article_code_key][timestamp_key] = row
        return table

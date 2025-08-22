from typing import Any, Generator, Protocol


class SampleGenerator(Protocol):
    def points_of_sale(self) -> Generator[dict[str, Any], None, None]: ...

    def articles_by_point_of_sale(self) -> Generator[dict[str, Any], None, None]: ...

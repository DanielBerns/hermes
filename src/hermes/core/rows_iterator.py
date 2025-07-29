from collections import defaultdict
from typing import Any, Generator, List, Protocol


class RowsIterator(Protocol):
    def rows(self, table: defaultdict[str, List[dict[str, Any]]]) -> Generator[dict[str, Any], None, None]:
        ...

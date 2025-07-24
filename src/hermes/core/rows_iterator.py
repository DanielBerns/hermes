from typing import Any, List, Protocol, Generator
from collections import defaultdict

class RowsIterator(Protocol):
    def rows(self, table: defaultdict[str, List[dict[str, Any]]]) -> Generator[dict[str, Any], None, None]:
        ...

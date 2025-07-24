import logging
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Tuple, Generator
from unpsjb_fce_obsecon.utils.helpers import (
    classname,
    get_container,
    get_directory,
    get_resource,
    get_timestamp
)
from unpsjb_fce_obsecon.utils.metadata import Metadata


# Get a named logger for this module
logger = logging.getLogger(__name__)


class TreeStoreException(Exception):
    """Exception for the tree_store module."""
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


@dataclass
class Leaf:
    home: Path
    index: int
    key: str
    timestamp: str



MAX_INDEX_VALUE = 16777216


def base_256(number: int) -> Tuple[int, int, int]:
    """
    Converts a non-negative integer to a base-256 representation.

    Args:
        number: The integer to convert (must be between 0 and 16777215 inclusive).

    Returns:
        A tuple of three integers representing the base-256 digits
        (most significant to least significant).

    Raises:
        TreeStoreException: If the input number is out of range.
    """
    test = 0 <= number < MAX_INDEX_VALUE
    if not test:
        message = f"Input number must be between 0 and {MAX_INDEX_VALUE}. Got {number}"
        raise TreeStoreException(message)
    third_digit = number % 256
    number >>= 8  # Equivalent to number //= 256
    second_digit = number % 256
    first_digit = number >> 8  # Equivalent to number //= 256
    return first_digit, second_digit, third_digit


def base_10(first_digit: int, second_digit: int, third_digit: int) -> int:
    """
    Converts a three-digit base-256 number to its base-10 representation.

    Args:
        first_digit: The most significant digit (0-255).
        second_digit: The middle digit (0-255).
        third_digit: The least significant digit (0-255).

    Returns:
        The integer representation of the base-256 number.
    """
    return (((first_digit << 8) + second_digit) << 8) + third_digit


def get_leaf_tuple(number: int) -> Tuple[str, str, str]:
    """
    Generates a tuple of 3 digit base-256 number from an integer, represented as zero padded strings

    Args:
        number: The integer to convert (must be between 0 and 16777215 inclusive).

    Returns:
        Three 3-character strings  (e.g., "000 000 000", "001002003").
    """
    first_digit, second_digit, third_digit = base_256(number)
    return (f"{first_digit:03d}", f"{second_digit:03d}", f"{third_digit:03d}")


class Index:
    """
    Manages an integer index stored in a JSON file.

    This class provides methods to read, write, and increment an index value
    that is persisted in a JSON file.  It's used to keep track of the
    current leaf number in the TreeStore.
    """
    def __init__(self, resource: Path) -> None:
        """
        Initializes the Index object.

        Args:
            resource: The path to the JSON file where the index is stored.
        """
        self._resource: Path = resource
        self._value: int = 0  # Initialize with a default value

        if self.resource.exists():
            # Restarting Index: Load existing value.
            try:
                value = self.read()
                self._value = int(value.get("index", 0)) # Use .get() for safer access
            except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
                logging.error(f"Error reading index file {self.resource}: {e}")
                raise TreeStoreException(f"Could not read or parse index file {self.resource}") from e
        else:
            # Initialize Index: Start from 0 and write to file
            self.write()

    @property
    def resource(self) -> Path:
        """
        Returns the path to the index file.

        Returns:
            Path: The path to the JSON file.
        """
        return self._resource

    @property
    def value(self) -> int:
        """
        Returns the current value of the index.

        Returns:
            int: The current index value.
        """
        return self._value

    @value.setter
    def value(self, update: int) -> None:
        """
        Sets the index to a new value and writes it to the file.

        Args:
            update: The new value for the index.
        """
        if self._value >= update:
            message = f"Index update {update} must be greater than {self._value}"
            raise TreeStoreException(message)
        self._value = update
        self.write()

    def write(self) -> None:
        """
        Writes the current index value to the JSON file.
        """
        value = {"index": str(self.value)}
        try:
            with open(self.resource, "w") as target:
                json.dump(value, target, indent=4) # Use indent for readability
        except IOError as e:
            message = f"Error writing index file {self.resource}: {e}"
            raise TreeStoreException(message)


    def read(self) -> dict[str, str]:
        """
        Reads the index value from the JSON file.

        Returns:
            dict[str, str]: A dictionary containing the index value (as a string).

        Raises:
            TreeStoreException: If the index file cannot be read or parsed.
        """
        try:
            with open(self.resource, "r") as source:
                values = json.load(source)
            return values
        except FileNotFoundError:
            message = f"Index file not found: {self.resource}"
            raise TreeStoreException(message)
        except json.JSONDecodeError as e:
            message = f"Error decoding JSON from index file {self.resource}: {e}"
            raise TreeStoreException(message)
        except IOError as e:
            message = f"Error reading index file {self.resource}: {e}"
            raise TreeStoreException(message)


class TreeStore:
    """
    Manages a hierarchical directory structure for storing leafs.

    The TreeStore organizes leafs into directories named using a 9-digit
    zero-padded identifier. It uses an Index object to keep track of the
    next available leaf identifier.  The structure is as follows:

    ```
    <base_path>/
        <identifier>/  (e.g., my_data)
            index.json
            leafs/
                000/
                    000/
                       000/
                           ... (leaf data) ...
                       001/
                           ... (leaf data) ...
                       ...
                       255/
                    001/
                       000/
                           ... (leaf data) ...
                       001/
                           ... (leaf data) ...
                       ...
                       255/
    ```
    """
    def __init__(self, base: Path, identifier: str) -> None:
        """
        Initializes a TreeStore object.

        Args:
            base: The base directory where the TreeStore will be created.
            identifier: A string identifier for this specific TreeStore instance.
        """
        self._home = get_container(base, identifier)
        self._root = get_container(self.home, "root")
        resource = get_resource(self.home, "index", ".json")
        self._index = Index(resource)

    @property
    def home(self) -> Path:
        """
        Returns the home directory of the TreeStore.

        Returns:
           Path: The path to the TreeStore's home directory.
        """
        return self._home

    @property
    def root(self) -> Path:
        """
        Returns the directory containing the root directory,
            where you find the branches of the tree.

        Returns:
            Path: The path to the root directory.
        """
        return self._root

    @property
    def index(self) -> Index:
        """
        Returns the Index object associated with this TreeStore.

        Returns:
             Index: The Index object.
        """
        return self._index

    def create_leaf(self) -> Leaf:
        """
        Creates a new leaf directory and increments the index.

        Returns:
            Path: The path to the newly created leaf directory.
        """
        leaf_index = self.index.value
        (first_digit, second_digit, third_digit) = get_leaf_tuple(leaf_index)
        leaf_home = get_directory(Path(self.root, first_digit, second_digit, third_digit))
        self.index.value += 1
        leaf_key = f"{first_digit}{second_digit}{third_digit}"
        leaf_timestamp = get_timestamp()
        metadata = Metadata(leaf_home)
        metadata.add("key", leaf_key)
        metadata.add("timestamp", leaf_timestamp)
        metadata.write()
        return Leaf(leaf_home, leaf_index, leaf_key, leaf_timestamp)

    def _get_leaf(self, leaf_index: int) -> Leaf:
        (first_digit, second_digit, third_digit) = get_leaf_tuple(leaf_index)
        leaf_home = get_directory(Path(self.root, first_digit, second_digit, third_digit))
        leaf_key = f"{first_digit}{second_digit}{third_digit}"
        metadata = Metadata(leaf_home)
        metadata.read()
        leaf_timestamp = metadata.table.get("timestamp", False)
        if not leaf_timestamp:
            leaf_timestamp = metadata.table.get("sample_key", "00000000000000")
        return Leaf(leaf_home, leaf_index, leaf_key, leaf_timestamp)

    def get_leaf(self, leaf_index: int) -> Leaf:
        if 0 <= leaf_index < self.index.value:
            return self._get_leaf(leaf_index)
        else:
            raise TreeStoreException(
                f"{classname(self)}.get_leaf: out of range leaf_index {leaf_index} | {self.index.value}"
            )

    def iterate(self, first: int = 0, top: int = -1) -> Generator[Leaf, None, None]:
        """
        get a sequence of leaf_index where first <= leaf_index < top

        Args:
            first: Determine the first leaf directory of the selection,
            top: Determine the last leaf directories to select over.
                 If -1 (default), includes the last leaf directory

        Yields:
            leaf: a Leaf instance containing all the leaf parameters.

        Raises:
            TreeStoreException: If `first < 0` or `top < first` or  `top >= index.value`
        """
        if first < 0:
            explanation = (
                f"{classname(self):s}.leafs:"
                f"first {first:d} must be non negative"
            )
            raise TreeStoreException(explanation)
        if top == -1:
            top = self.index.value
        elif first <= top <= self.index.value:
            pass  # Valid top value
        else:
            explanation = (
                f"{classname(self):s}.leafs: "
                f"bad range [first: {first:d} - top: {top:d} - index: {self.index.value:d})]"
            )
            raise TreeStoreException(explanation)
        for leaf_index in range(first, top):
            yield self._get_leaf(leaf_index)


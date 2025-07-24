import logging
from collections import defaultdict
from pathlib import Path
import re

from unpsjb_fce_obsecon.utils.helpers import get_resource, read_text_file, create_text_file

# Use logging.getLogger, which is the standard way to get a logger instance.
logger = logging.getLogger(__name__)


class ParametersException(Exception):
    """Custom exception for errors during parameter processing."""
    # No changes needed here, but it's good practice to have custom exceptions.
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


class Switches:
    """
    Manages reading and writing switch-like configuration parameters
    from a structured text file.

    The file format is:
    # Group Name 1
    1. Switch Name A # True
    2. Switch Name B # False
    # Group Name 2
    1. Switch Name C # True
    """

    def __init__(self, identifier: str) -> None:
        """
        Initializes the Switches object.

        Args:
            identifier: A name used to construct the filename (e.g., 'settings').
        """
        self._identifier = identifier
        self._table: defaultdict[str, dict[str, bool]] = defaultdict(dict)
        # Updated regex for the new format (no leading '#').
        # It now matches lines that start with a number and a dot.
        self._line_regex = re.compile(r"^\s*\d+\.\s*(?P<key>.+?)\s*#\s*(?P<value>True|False)\s*$")

    @property
    def identifier(self) -> str:
        """Returns the identifier for this set of switches."""
        return self._identifier

    @property
    def table(self) -> defaultdict[str, dict[str, bool]]:
        """Returns the internal data table."""
        return self._table

    def write(self, container: Path) -> None:
        """
        Writes the current switch table to a text file using the new format.

        Args:
            container: The directory (as a Path object) where the file will be saved.
        """
        resource = self.get_resource(container, self.identifier, ".txt")

        with create_text_file(resource) as text:
            for group_name, group_components in sorted(self.table.items()):
                text.write(f"# {group_name}\n")
                for i, (component_name, component_value) in enumerate(group_components.items(), 1):
                    text.write(f"{i}. {component_name} # {component_value}\n")

    def read(self, container: Path) -> None:
        """
        Reads and parses a switch configuration file using the new format.

        Args:
            container: The directory (as a Path object) where the file is located.

        Raises:
            ParametersException: If the file format is invalid.
        """

        # Updated regex for the new format (no leading '#').
        # It now matches lines that start with a number and a dot.
        line_regex = re.compile(r"^\s*\d+\.\s*(?P<key>.+?)\s*#\s*(?P<value>True|False)\s*$")

        resource = self.get_resource(container, self.identifier, ".txt")

        if not resource.exists():
            raise ParametersException(f"Resource file not found at: {resource}")

        # self.table.clear() # Clear existing data before reading
        current_group = ""

        with read_text_file(resource) as text:
            for line_number, line in enumerate(text, 1):
                line = line.strip()
                if not line:
                    continue
                # A line starting with '# ' is a group header.
                if line.startswith("# "):
                    current_group = line[2:].strip()
                    if not current_group:
                        raise ParametersException(f"Invalid group name at line {line_number}: '{line}'")
                    # self.table[current_group] = {}
                else: # Otherwise, it should be a component line.
                    if not current_group:
                        raise ParametersException(f"Component found before any group definition at line {line_number}: '{line}'")

                    match = line_regex.match(line)
                    if not match:
                        raise ParametersException(f"Malformed component line at {line_number}: '{line}'")

                    key = match.group('key').strip()
                    value_str = match.group('value').strip()
                    value = value_str == "True"

                    self.table[current_group][key] = value

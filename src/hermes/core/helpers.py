"""
This module provides utility functions for working with files, directories,
timestamps, and environment variables. It includes functions for:

- Reading and writing JSON and text files.
- Creating and removing directories.
- Generating timestamps.
- Getting environment variables.
- Working with file paths in a platform-independent way.

The module uses the `pathlib` library for path manipulation, which provides
a more object-oriented approach compared to the older `os.path` module.  It
also includes type hints for improved code clarity and maintainability.

"""

import json
import os
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from shutil import rmtree
from typing import Any, Generator, TextIO, Tuple

indentation = "    "  # Define an indentation constant.


def classname(thing: Any) -> str:
    """Returns the name of the class of an object.

    Args:
        thing: The object whose class name is to be retrieved.

    Returns:
        The name of the class of the object as a string.
    """
    return thing.__class__.__name__


def i(n: int) -> str:
    """Returns a string of spaces representing indentation.

    Args:
        n: The number of indentation levels.

    Returns:
        A string containing `n` times the `indentation` string.
    """
    return indentation * n


def aware_utcnow(offset: timedelta) -> datetime:
    """Returns an aware datetime object representing the current UTC time with
    the given offset.

    Args:
        offset: A timedelta object representing the timezone offset.

    Returns:
        An aware datetime object representing the current UTC time
        with the specified offset.
    """
    return datetime.now(timezone(offset))


DEFAULT_TIME_DELTA = -timedelta(hours=3)  # Default timedelta offset


def get_timestamp(separator: str = "", offset: timedelta = DEFAULT_TIME_DELTA) -> str:
    """Generates a timestamp string in the format YYYYMMDDHHMMSS.

    Args:
        separator: An optional string to use as a separator between date/time
            parts. Defaults to an empty string.
        offset: A timedelta object representing the timezone offset.
            Defaults to DEFAULT_TIME_DELTA (-3 hours).

    Returns:
        A string representing the timestamp.
    """
    now = aware_utcnow(offset)
    timestamp = separator.join(
        [
            f"{now.year:4d}",
            f"{now.month:02d}",
            f"{now.day:02d}",
            f"{now.hour:02d}",
            f"{now.minute:02d}",
            f"{now.second:02d}",
        ]
    )
    return timestamp


class HelpersException(Exception):
    def __init__(self, identifier: str, message: str) -> None:
        super().__init__(f"{__file__}: {message}")


@contextmanager
def create_text_file(resource: Path) -> Generator[TextIO, None, None]:
    try:
        with open(resource, "w", encoding="utf-8") as target_file:
            yield target_file
    except Exception as e:
        message = f"Error with text file {str(resource):s}: {str(e):s}"
        raise HelpersException("create_text_file", message)


@contextmanager
def read_text_file(resource: Path) -> Generator[TextIO, None, None]:
    try:
        with open(resource, "r", encoding="utf-8") as text_file:
            yield text_file
    except Exception as e:
        message = f"Error with text file {str(resource):s}: {str(e):s}"
        raise HelpersException("read_text_file", message)


def read_json(resource: Path) -> dict[str, Any] | None:
    """Reads a JSON file and returns its contents as a dictionary.

    Args:
        resource: The path to the JSON file.

    Returns:
        A dictionary containing the data from the JSON file.
        Returns None if the file could not be read.
    """
    with read_text_file(resource) as origin:
        return json.load(origin)


def write_json(resource: Path, data: dict[str, Any]) -> None:
    """Writes a dictionary to a JSON file.

    Args:
        resource: The path to the JSON file.
        data: The dictionary to be written.
    """
    with create_text_file(resource) as target:
        json.dump(data, target)


def read_text(resource: Path) -> str:
    """Reads the entire contents of a text file encoded in UTF-8.

    Args:
        resource: The path to the text file.

    Returns:
        The contents of the file as a string.
    """
    # get text from utf-8 encoded file
    with read_text_file(resource) as origin:
        return origin.read()


def write_text(resource: Path, text: str) -> None:
    """Writes a string to a text file encoded in UTF-8.

    Args:
        resource: The path to the text file.
        text: The string to be written.
    """
    with create_text_file(resource) as target:
        target.write(text)


def read_text_lines(resource: Path) -> Generator[str, None, None]:
    """Reads a text file line by line, yielding each line without the trailing
    newline character.  Uses UTF-8 encoding.

    Args:
        resource: The path to the text file.

    Yields:
        Each line of the file as a string, with the trailing newline removed.
    """
    with read_text_file(resource) as origin:
        yield from (line[:-1] for line in origin)


def read_json_lines(resource: Path) -> Generator[dict[str, Any], None, None]:
    for a_line in read_text_lines(resource):
        yield json.loads(a_line)


def as_jsonl(event: dict[str, str]) -> str:
    return json.dumps(event) + "\n"


def write_text_lines(resource: Path) -> Generator[int, str, None]:
    """A generator that writes lines of text to a file.  Uses UTF-8 encoding.

    The generator receives lines of text to write via its `send()` method.
    It yields the number of lines written so far. The generator must be
    initialized by sending `None` or calling `next()` on it. The generator
    should be closed by sending `None` after the last line has been sent.

    Args:
        resource: The path to the file to write to.

    Yields:
        The number of lines written to the file (including the current line).

    Receives:
        str: The next line of text to write to the file.  Send `None` to
            close the file and terminate the generator.

    Example:
        writer = write_text_lines(Path("my_file.txt"))
        next(writer)  # Initialize the generator
        writer.send("First line")
        count = writer.send("Second line")
        print(f"Lines written: {count}")  # Output: Lines written: 2
        writer.send(None) # Close and terminate
    """
    number = 0
    with create_text_file(resource) as target:
        text_line = yield number  # Initial yield, returns 0
        if text_line:
            number += 1
            target.write(text_line + "\n")
            while True:
                text_line = yield number
                if text_line is None:
                    break
                number += 1
                target.write(text_line + "\n")


def get_directory(base: Path) -> Path:
    """Ensures the existence of a directory and returns its path.

    If the directory does not exist, it is created, including any necessary
    parent directories.  The created directory has permissions set to 0o700
    (read, write, and execute for the owner only).  If the path represents
    a symbolic link to the user's home directory, it is expanded.

    Args:
        base: The path to the directory.

    Returns:
        The absolute path to the directory.
    """
    directory = base.expanduser()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    return directory


def get_container(base: Path, identifier: str):
    """Gets a subdirectory within a base directory.

    This is a convenience function that calls `get_directory` with a path
    constructed from the base path and the identifier.

    Args:
        base: The base directory.
        identifier: The name of the subdirectory.

    Returns:
        The path to the subdirectory.
    """
    return get_directory(Path(base, identifier))


def get_resource(base: Path, name: str, suffix: str) -> Path:
    """Constructs a resource path within a directory.

    Creates the directory if it doesn't exist, and returns a `Path` object
    representing a file with the given name and suffix within that directory.

    Args:
        base: The base directory.
        name: The name of the file (without the suffix).
        suffix: The file suffix (including the leading dot, e.g., ".txt").

    Returns:
        A `Path` object representing the resource.
    """
    return Path(get_directory(base), name).with_suffix(suffix)


def get_resource_with_timestamp(base: Path, name: str, suffix: str) -> Path:
    """Constructs a resource path with a timestamp appended to the name.

    The timestamp is in the format YYYYMMDDHHMMSS and is appended to the
    filename before the suffix.

    Args:
        base: The base directory.
        name: The base name of the file.
        suffix: The file suffix (including the leading dot).

    Returns:
        A `Path` object representing the resource with the timestamp.
    """
    timestamp = get_timestamp()
    resource = get_resource(base, f"{name:s}_{timestamp:s}", suffix)
    return resource


def remove_directory(base: Path) -> None:
    """Removes a directory and its contents recursively.

    Args:
        base: The path to the directory to be removed.
    """
    rmtree(base)


def get_environment_variable(name: str, default_value: str = "") -> str:
    """Retrieves the value of an environment variable.

    Args:
        name: The name of the environment variable.
        default_value: The value to return if the environment variable is not
            set. Defaults to "".

    Returns:
        The value of the environment variable, or the default value if it is
        not set.
    """
    return os.environ.get(name) or default_value


def erase_directory_contents(directory: Path) -> None:
    """Erases all files and subdirectories within the specified directory.
    Args:
        directory pathlib.Path The path to the directory whose contents should be erased.
    Raises:
        FileNotFoundError: If the provided directory does not exist.
        TypeError: If the directory isn't a pathlib.Path.
        PermissionError: If you do not have the correct permissions.
    """

    if not directory.exists():
        raise FileNotFoundError(f"The directory '{directory}' does not exist.")

    if not directory.is_dir():
        raise ValueError(f"The path '{directory}' is not a directory.")

    for item in directory.iterdir():
        if item.is_file():
            item.unlink()  # Delete files
        elif item.is_dir():
            rmtree(item)  # Delete subdirectories and their contents recursively
        elif item.is_symlink():
            item.unlink()  # Delete symbolic links


def measure_execution_time(long_running_task: Callable[[], int]) -> Tuple[int, float]:
    """
    Measures the execution time of a function.

    Args:
        long_running_task: The function to measure.

    Returns:
        A tuple containing the return value of the function and the execution time in seconds.
    """
    start_time = time.time()
    result = long_running_task()
    end_time = time.time()
    execution_time = end_time - start_time
    return result, execution_time

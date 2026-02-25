from datetime import datetime, timedelta, timezone
from random import randint
import time


# ARG represents the time difference for Argentina's standard time (UTC-3)
UTC = timedelta(hours=0)
ARG = timedelta(hours=-3)


def random_sleep(base: int = 5, lo: int = 1, hi: int = 9) -> None:
    """
    Sleeps for a random duration within a specified range.

    Args:
        base: The base sleep duration in seconds.
        lo: The lower bound of the random sleep duration (added to base).
        hi: The upper bound of the random sleep duration (added to base).
    """
    time.sleep(base + randint(lo, hi))


def aware_utcnow(offset: timedelta = ARG) -> datetime:
    """
    Returns the current UTC time with a specified timezone offset.

    Args:
        offset: A timedelta object representing the timezone offset.

    Returns:
        A datetime object representing the current time with the given offset.
    """
    return datetime.now(timezone(offset))


def get_timestamp(separator: str = "", offset: timedelta = ARG) -> str:
    """
    Generates a timestamp string in the format YYYYMMDDHHMMSS.

    Args:
        separator: The separator to use between the date and time parts. Defaults to "".
        offset: A timedelta object representing the timezone offset. Defaults to ARG.

    Returns:
        A string representing the timestamp.
    """
    now = aware_utcnow(offset)
    timestamp = separator.join(
        [
            f"{now.year:4d}{now.month:02d}",
            f"{now.day:02d}{now.hour:02d}",
            f"{now.minute:02d}",
            f"{now.second:02d}",
        ]
    )
    return timestamp

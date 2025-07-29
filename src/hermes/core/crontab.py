import logging
import subprocess
from pathlib import Path

from hermes.core.helpers import create_text_file

# https://www.adminschoice.com/crontab-quick-reference


logger = logging.getLogger(__name__)


class CrontabException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        logger.error(message)


def check_command(command: str) -> str:
    item = Path(command).expanduser()
    if item.exists():
        return str(item)
    else:
        explanation = f"crontab.check_command: command doesn't exists {str(item):s}"
        raise CrontabException(explanation)


def check_minute(minute: str) -> str:
    if minute:
        if minute == "*":
            return minute
        try:
            if 0 <= int(minute) < 60:
                return minute
            else:
                explanation = f"crontab.check_minute: invalid minute range: {minute:s}"
                raise CrontabException(explanation)
        except ValueError:
            explanation = f"crontab.check_minute: invalid minute value: {minute:s}"
            raise CrontabException(explanation)
    else:
        explanation = "crontab.check_minute: invalid minute length"
        raise CrontabException(explanation)


def check_hour(hour: str) -> str:
    if hour:
        if hour == "*":
            return hour
        try:
            if 0 <= int(hour) <= 23:
                return hour
            else:
                explanation = f"crontab.check_hour: invalid hour range: {hour:s}"
                raise CrontabException(explanation)
        except ValueError:
            explanation = f"crontab.check_hour: invalid hour value: {hour:s}"
            raise CrontabException(explanation)
    else:
        explanation = "crontab.check_hour: invalid hour length"
        raise CrontabException(explanation)


def check_day_of_month(day_of_month: str) -> str:
    if day_of_month:
        if day_of_month == "*":
            return day_of_month
        try:
            if 1 <= int(day_of_month) <= 31:
                return day_of_month
            else:
                explanation = f"crontab.check_day_of_month: invalid day_of_month range: {day_of_month:s}"
                raise CrontabException(explanation)
        except ValueError:
            explanation = f"crontab.check_day_of_month: invalid day_of_month value: {day_of_month:s}"
            raise CrontabException(explanation)
    else:
        explanation = "crontab.check_day_of_month: invalid day_of_month length"
        raise CrontabException(explanation)


def check_month(month: str) -> str:
    if month:
        if month == "*":
            return month
        try:
            if 1 <= int(month) <= 12:
                return month
            else:
                explanation = f"crontab.check_hour: invalid month range: {month:s}"
                raise CrontabException(explanation)
        except ValueError:
            explanation = f"crontab.check_hour: invalid month value: {month:s}"
            raise CrontabException(explanation)
    else:
        explanation = "crontab.check_hour: invalid month length"
        raise CrontabException(explanation)


def check_day_of_week(day_of_week: str) -> str:
    if day_of_week:
        if day_of_week == "*":
            return day_of_week
        try:
            if 0 <= int(day_of_week) <= 6:
                return day_of_week
            else:
                explanation = f"crontab.check_hour: invalid day_of_week range: {day_of_week:s}"
                raise CrontabException(explanation)
        except ValueError:
            explanation = f"crontab.check_hour: invalid day_of_week value: {day_of_week:s}"
            raise CrontabException(explanation)
    else:
        explanation = "crontab.check_hour: invalid day_of_week length"
        raise CrontabException(explanation)


class Crontab:
    def __init__(self):
        self._table: dict[str, str] = {}

    @property
    def table(self) -> dict[str, str]:
        return self._table

    def add(
        self,
        key: str,
        command: str,
        timing: str
    ) -> None:
        parts = timing.split(' ')
        if len(parts) != 5:
            explanation = f"{self.__class__.__name__} Bad timing: {key} - {command} - {timing}"
            raise CrontabException(explanation)
        minute: str = parts[0]
        hour: str = parts[1]
        day_of_month: str = parts[2]
        month: str = parts[3]
        day_of_week: str = parts[4]
        try:
            _command = check_command(command)
            _minute = check_minute(minute)
            _hour = check_hour(hour)
            _day_of_month = check_day_of_month(day_of_month)
            _month = check_month(month)
            _day_of_week = check_day_of_week(day_of_week)
        except CrontabException:
            explanation = " ".join(
                    (   self.__class__.__name__,
                        "erroneous parameters:",
                        command,
                        minute,
                        hour,
                        day_of_month,
                        month,
                        day_of_week,
                    )
            )
            raise CrontabException(explanation)
        else:
            line = " ".join(
                (_minute, _hour, _day_of_month, _month, _day_of_week, _command)
            )
            self.table[key] = line

    def content(self):
        return "\n".join(line.strip() for line in self.table.values())

    def update(self, crontab_txt: Path) -> None:
        with create_text_file(crontab_txt) as target:
            target.write(self.content() + "\n")
        cp = subprocess.run(["crontab", str(crontab_txt)])
        if cp.returncode:
            logger.info(f"Crontab.update({crontab_txt}) returncode {cp.returncode:d}")

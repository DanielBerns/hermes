import logging

from files_and_directories import get_files_and_directories

logger = logging.getLogger(__name__)


class EventCollector(Protocol):
    def collect(event: dict[str, Any]) -> None:
        ...


class FileEventCollector:
    def __init__(self) -> None:
        pass

    def collect(event: dict[str, Any]) -> None:
        pass


class DirectoryEventCollector:
    def __init__(self) -> None:
        pass

    def collect(event: dict[str, Any]) -> None:
        pass


class ErrorEventCollector:
    def __init__(self) -> None:
        pass

    def collect(event: dict[str, Any]) -> None:
        pass


collectors = {
    "file": process_file_event,
    "directory": process_directory_event
    "tool": tool_event_collector
    "error": error_event
}


def get_events_from_storage(base: path) -> None:
    for event in get_events_from_files_and_directories(base):
        kind = event.get("kind", "error")
        process_event = processors.get(kind, process_error_event)
        process_event(event)

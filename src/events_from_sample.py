import os
from pathlib import Path
from typing import Generator, Any
from helpers import read_text_file


def build_error(
    context: str,
    base: Path,
    an_exception: Exception | None,
    a_path: Path,
    a_message: str
) -> dict[str, Any]:
    return {
        "kind": "error",
        "context": context,
        "base": base,
        "exception": an_exception,
        "path": entry
        "message": a_message
    }


def get_events_from_metadata(
    context: str,
    base: Path
) -> Generator[dict[str, Any], None, None]:
    errors = []
    metadata_directory = Path(base, '.metadata')
    if metadata_directory.exists():
        for name in os.listdir(metadata_directory):
            entry = Path(metadata_directory, name)
            if entry.is_file():
                suffix = entry.suffix
                name = entry.name[:-len(suffix)]
                if suffix = ".json":
                    try:
                       with read_text_file(entry) as text:
                           event = json.load(text)
                           event["kind"] = "tool.parameters"
                           event["tool"] = name
                           event["base"] = base
                           yield event
                    except Exception as e:
                        event = build_error(context, base, e, entry, "no json content")
                        yield event
    else:
        event = build_error(context, base, None, metadata_directory, "no metadata_directory")
        yield event


def get_events_from_tree_store(
    context: str,
    base: Path
) -> Generator[dict[str, Any], None, None]:


def get_events_from_components(
    context: str,
    base: Path
) -> Generator[dict[str, Any], None, None]:
    errors = []
    for name in os.listdir(base):
        if name == "tree_store":
            entry = Path(base, name)
            if entry.is_dir():
                yield from get_events_from_tree_store(context, entry)
            else:
                event = build_error(context, base, None, entry, "no tree_store")
                yield event
        else:
            entry = Path(base, name)
            if entry.is_file():
                file_path = Path(base, a_filename)
                suffix = file_path.suffix
                name = file_path.name[:-len(suffix)]
                event = {
                    "kind": "file",
                    "name": name,
                    "suffix": suffix,
                    "context": context,
                    "file_path": file_path
                }
                yield event


def get_events_from_sample(
    base: Path
) -> Generator[dict[str, Any], None, None]:
    context = base.name
    yield from get_events_from_metadata(context, base)
    yield from get_events_from_components(context, base)

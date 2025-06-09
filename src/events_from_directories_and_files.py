import os
from pathlib import Path
from typing import Generator, Any


def get_events_from_directories_and_files(
    base: Path
) -> Generator[dict[str, Any], None, None]:
    for (root, directories, filenames) in os.walk(base):
        for directory_name in directories:
            directory_path = Path(root, directory_name)
            name = directory_path.name
            event = {
                "kind": "directory",
                "name": name,
                "root": root,
                "directory_path": directory_path
            }
            yield event

        for a_filename in filenames:
            file_path = Path(root, a_filename)
            suffix = file_path.suffix
            name = file_path.name[:-len(suffix)]
            event = {
                "kind": "file",
                "name": name,
                "suffix": suffix,
                "root": root,
                "file_path": file_path
            }
            yield event


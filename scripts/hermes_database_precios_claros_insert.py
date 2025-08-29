from pathlib import Path

from hermes.core.action import execute
from hermes.domain.database_insert import DatabaseInsert


def main() -> None:
    filename = Path(__file__)
    script, project_identifier = filename.stem, filename.parents[1].stem
    action = DatabaseInsert()
    execute(script, project_identifier, action)


if __name__ == "__main__":
    main()

from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from hermes.core.constants import (DATABASE, DATABASE_DEFAULT, DOTENV,
                                   INSTANCE, INSTANCE_BASE, LOG_LEVEL,
                                   LOG_LEVEL_DEFAULT, PROJECT, VERSION,
                                   VERSION_DEFAULT)
from hermes.core.helpers import (create_text_file, get_container,
                                 get_directory, get_environment_variable)

# No logging in this module
# Logging will start after load_config invocation





def load_config(
    info_path: Path,
    project: str,
    instance: str
) -> dict[str, Any]:
    version = get_environment_variable(VERSION, VERSION_DEFAULT)
    instance_base = get_directory(info_path / project / version / instance)
    instance_dotenv_file = get_container(instance_base, DOTENV)

    config: dict[str, Any] = {}
    if not instance_dotenv_file.exists():
        print(f"Warning: dotenv file not found at {instance_dotenv_file}")
        with create_text_file(instance_dotenv_file) as text_file:
            text_file.write("# settings\n")
            text_file.write(f"{DATABASE}={DATABASE_DEFAULT}\n")
            text_file.write(f"{LOG_LEVEL}={LOG_LEVEL_DEFAULT}\n")
    else:
        pass
    load_dotenv(dotenv_path=instance_dotenv_file)
    config[PROJECT] = project
    config[VERSION] = version
    config[INSTANCE] = instance
    config[INSTANCE_BASE] = instance_base
    # Load environment variables into your config dictionary or object
    config[DATABASE] = get_environment_variable(DATABASE, DATABASE_DEFAULT)
    config[LOG_LEVEL] = get_environment_variable(LOG_LEVEL, LOG_LEVEL_DEFAULT)

    return config


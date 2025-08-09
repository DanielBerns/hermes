from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from hermes.core.constants import (DATABASE, DATABASE_DEFAULT, DOTENV,
                                   INSTANCE, INSTANCE_BASE, LOG_LEVEL, INFO, INFO_BASE,
                                   LOG_LEVEL_DEFAULT, PROJECT, VERSION,
                                   VERSION_DEFAULT, SECRETS, SECRETS_BASE)
from hermes.core.helpers import (create_text_file, get_container,
                                 get_directory,
                                 get_resource,
                                 get_environment_variable)

# No logging in this module
# Logging will start after load_config invocation

def load_config(
    info_root: Path,
    secrets_root: Path,
    project: str,
    instance: str
) -> dict[str, Any]:
    version = get_environment_variable(VERSION, VERSION_DEFAULT)
    info_base = get_directory(info_root / project / version / instance)
    secrets_base = get_directory(secrets_root / project / version / instance)
    instance_dotenv_file = get_resource(info_base, DOTENV, "")

    config: dict[str, Any] = {}
    if not instance_dotenv_file.exists():
        print(f"Warning: dotenv file not found at {instance_dotenv_file}")
        with create_text_file(instance_dotenv_file) as text_file:
            text_file.write("# settings\n")
            text_file.write(f"{LOG_LEVEL}={LOG_LEVEL_DEFAULT}\n")
    else:
        pass
    load_dotenv(dotenv_path=instance_dotenv_file)
    config[PROJECT] = project
    config[VERSION] = version
    config[INSTANCE] = instance
    config[INFO_BASE] = info_base
    config[SECRETS_BASE] = secrets_base

    # Load environment variables into your config dictionary or object
    config[LOG_LEVEL] = get_environment_variable(LOG_LEVEL, LOG_LEVEL_DEFAULT)

    return config


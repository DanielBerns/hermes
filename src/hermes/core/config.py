from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from hermes.core.constants import (
    INFO_BASE_KEY,
    SECRETS_BASE_KEY,
    PROJECT_KEY,
    VERSION_LABEL_KEY,
    VERSION_LABEL,
    VERSION_STORAGE_KEY,
    VERSION_STORAGE_DEFAULT,
    INSTANCE_KEY,
    LOG_LEVEL_KEY,
    LOG_LEVEL_DEFAULT,
    DOTENV,
    DATABASE_NAME_KEY
)
from hermes.core.helpers import (
    create_text_file,
    get_directory,
    get_resource,
    get_environment_variable,
)

# No logging in this module
# Logging will start after load_config invocation


def load_config(
    info_root: Path, secrets_root: Path, project: str, instance: str
) -> dict[str, Any]:
    version_storage = get_environment_variable(
        VERSION_STORAGE_KEY, VERSION_STORAGE_DEFAULT
    )
    info_base = get_directory(info_root / project / version_storage / instance)
    secrets_base = get_directory(secrets_root / project / version_storage / instance)
    instance_dotenv_file = get_resource(info_base, DOTENV, "")

    config: dict[str, Any] = {}
    if not instance_dotenv_file.exists():
        print(f"Warning: dotenv file not found at {instance_dotenv_file}")
        with create_text_file(instance_dotenv_file) as text_file:
            text_file.write("# settings\n")
            text_file.write(f"{LOG_LEVEL_KEY}={LOG_LEVEL_DEFAULT}\n")
    else:
        pass
    # load settings
    load_dotenv(dotenv_path=instance_dotenv_file)
    # Load environment variables
    config[LOG_LEVEL_KEY] = get_environment_variable(LOG_LEVEL_KEY, LOG_LEVEL_DEFAULT)
    # Load filesystem variables
    config[PROJECT_KEY] = project
    config[VERSION_LABEL_KEY] = VERSION_LABEL
    config[VERSION_STORAGE_KEY] = version_storage
    config[INSTANCE_KEY] = instance
    config[INFO_BASE_KEY] = info_base
    config[SECRETS_BASE_KEY] = secrets_base

    return config

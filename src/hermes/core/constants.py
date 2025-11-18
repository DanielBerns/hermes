from pathlib import Path
from hermes.core.helpers import get_directory


INFO = "Info"
SECRETS = "Secrets"
LOGS = "logs"
MESSAGE_BOARD = "message_board"

PRODUCTION_CONFIG_PATH = "production_config_path"

INSTANCE_KEY = "instance"
DATABASE_NAME_KEY = "database_name"
LOG_LEVEL_KEY = "log_level"

YES = "yes"
NO = "no"

info_root = get_directory(Path.home() / INFO)
secrets_root = get_directory(Path.home() / SECRETS)

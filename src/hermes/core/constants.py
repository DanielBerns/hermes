from pathlib import Path
from hermes.core.helpers import get_directory


INFO = "Info"
SECRETS = "Secrets"
LOGS = "logs"
MESSAGE_BOARD = "message_board"

info_root = get_directory(Path.home() / INFO)
secrets_root = get_directory(Path.home() / SECRETS)

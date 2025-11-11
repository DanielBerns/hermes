import os
from pathlib import Path
from typing import Any, Tuple

import yaml
from pydantic import BaseModel, Field

# --- Pydantic Models for Type-Safe Configuration ---

class ProjectConfig(BaseModel):
    identifier: str = "hermes"
    version: Tuple[int, int, int] = [0, 1, 0]
    instance: str

class LoggingConfig(BaseModel):
    level: str = "INFO"
    timestamped: bool = True

class DatabaseConfig(BaseModel):
    name: str

class APIConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8000

class MessageBoardConfig(BaseModel):
    enabled: bool = False
    identifier: str = "hermes_dev"


class Config(BaseModel):
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    database: DatabaseConfig
    api: APIConfig = Field(default_factory=APIConfig)
    message_board: MessageBoardConfig = Field(default_factory=MessageBoardConfig)

# --- Configuration Loading Logic ---

def _load_config_from_file(path: Path) -> dict[str, Any]:
    """Loads a YAML configuration file."""
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)

def _merge_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merges two dictionaries."""
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _merge_configs(base[key], value)
        else:
            base[key] = value
    return base

import pdb

def get_config() -> Config:
    """
    Loads the application configuration from YAML files.

    The configuration is loaded in the following order:
    1. `base.yml` (default values)
    2. The file specified by the `HERMES_CONFIG_PATH` environment variable
       (environment-specific overrides)
    """
    config_dir = Path(__file__).parent.parent / "config"
    base_config = _load_config_from_file(config_dir / "base.yaml")

    env_config_path = os.getenv("HERMES_CONFIG_PATH")
    if env_config_path:
        env_config = _load_config_from_file(Path(env_config_path))
        merged_config = _merge_configs(base_config, env_config)
    else:
        # Load development.yml by default if no path is specified
        env_config = _load_config_from_file(config_dir / "development.yml")
        merged_config = _merge_configs(base_config, env_config)

    return Config(**merged_config)

# --- Global Config Object ---

config = get_config()

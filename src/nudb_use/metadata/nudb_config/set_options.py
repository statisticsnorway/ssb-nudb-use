from typing import Any

from nudb_config import settings as settings_use
from nudb_config.pydantic.load import NudbConfig


def set_option(setting_name: str, value: Any) -> NudbConfig:
    """Set an option in the options part of the nudb_config package.

    Args:
        setting_name: The name of the setting to set.
        value: The value we should set the setting to.

    Returns:
        NudbConfig: The changed config settings-object.
    """
    settings_use.options[setting_name] = value
    return settings_use

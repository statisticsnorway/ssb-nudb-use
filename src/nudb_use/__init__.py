"""NUDB Use - is a usage-package for the Norwegian National Education Database cloud-data. Both for data-consumers and data-deliverers. Requires access to NUDBs shared data in most instances."""

import importlib.metadata

from nudb_config import settings

from nudb_use.metadata import find_var
from nudb_use.metadata import find_vars
from nudb_use.metadata import get_dtypes
from nudb_use.metadata import update_colnames
from nudb_use.nudb_logger import LoggerStack
from nudb_use.nudb_logger import logger
from nudb_use.paths import get_periods_from_path
from nudb_use.paths import latest_shared_paths
from nudb_use.quality import run_quality_suite
from nudb_use.utils.packages import _check_ssb_nudb_config_version
from nudb_use.variables import derive

__all__ = [
    "LoggerStack",
    "derive",
    "find_var",
    "find_vars",
    "get_dtypes",
    "get_periods_from_path",
    "latest_shared_paths",
    "logger",
    "run_quality_suite",
    "settings",
    "update_colnames",
]


try:
    _check_ssb_nudb_config_version()
except Exception as err:
    logger.warning(f"Unable to validate `ssb-nudb-config` version!\nMessage: {err}")


try:
    try:
        __version__ = importlib.metadata.version(__name__)
    except Exception:
        __version__ = importlib.metadata.version("ssb-nudb-use")

except Exception as err:
    __version__ = "0.0.0"
    logger.warning(f"Unable to determine package version!\nMessage: {err}")
